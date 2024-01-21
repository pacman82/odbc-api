use std::{
    mem::swap,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

use crate::{buffers::ColumnarAnyBuffer, BlockCursor, Cursor, Error};

pub struct ConcurrentBlockCursor<C> {
    /// In order to avoid reallocating buffers over and over again, we use this channel to send the
    /// buffers back to the fetch thread after we copied their contents into arrow arrays.
    send_buffer: SyncSender<ColumnarAnyBuffer>,
    /// Receives filled batches from the fetch thread. Once the source is empty or if an error
    /// occurs its associated sender is dropped, and receiving batches will return an error (which
    /// we expect during normal operation and cleanup, and is not forwarded to the user).
    receive_batch: Receiver<ColumnarAnyBuffer>,
    /// We join with the fetch thread if we stop receiving batches (i.e. receive_batch.recv()
    /// returns an error) or `into_cursor` is called. `None` if the thread has already been joined.
    /// In this case either an error has been reported to the user, or the cursor is stored in
    /// `cursor`.
    fetch_thread: Option<JoinHandle<Result<C, Error>>>,
    /// Only `Some`, if the cursor has been consumed succesfully and `fetch_thread` has been joined.
    /// Can only be `Some` if `fetch_thread` is `None`. If both `fetch_thread` and `cursor` are
    /// `None`, it is implied that `fetch_thread` returned an error joining.
    cursor: Option<C>,
}

impl<C> ConcurrentBlockCursor<C>
where
    C: Cursor + Send + 'static,
{
    /// Construct a new concurrent block cursor.
    ///
    /// # Parameters
    ///
    /// * `block_cursor`: Taking a BlockCursor instead of a Cursor allows for better resource
    ///   stealing if constructing starting from a sequential Cursor, as we do not need to undbind
    ///   and bind the cursor.
    pub fn new(
        block_cursor: BlockCursor<C, ColumnarAnyBuffer>,
    ) -> Result<Self, Error> {
        let (send_buffer, receive_buffer) = sync_channel(1);
        let (send_batch, receive_batch) = sync_channel(1);

        let fetch_thread = thread::spawn(move || {
            let mut block_cursor = block_cursor;
            loop {
                match block_cursor.fetch_with_truncation_check(true) {
                    Ok(Some(_batch)) => (),
                    Ok(None) => {
                        break block_cursor
                            .unbind()
                            .map(|(undbound_cursor, _buffer)| undbound_cursor);
                    }
                    Err(odbc_error) => {
                        drop(send_batch);
                        break Err(odbc_error);
                    }
                }
                // There has been another row group fetched by the cursor. We unbind the buffers so
                // we can pass ownership of it to the application and bind a new buffer to the
                // cursor in order to start fetching the next batch.
                let (cursor, buffer) = block_cursor.unbind()?;
                if send_batch.send(buffer).is_err() {
                    // Should the main thread stop receiving buffers, this thread should
                    // also stop fetching batches.
                    break Ok(cursor);
                }
                // Wait for the application thread to give us a buffer to fill.
                match receive_buffer.recv() {
                    Err(_) => {
                        todo!();
                        // Application thread dropped sender and does not want more buffers to be
                        // filled. Let's stop this thread and return the cursor
                        break Ok(cursor);
                    }
                    Ok(next_buffer) => {
                        block_cursor = cursor.bind_buffer(next_buffer).unwrap();
                    }
                }
            }
        });

        Ok(Self {
            send_buffer,
            receive_batch,
            fetch_thread: Some(fetch_thread),
            cursor: None,
        })
    }

    pub fn into_cursor(self) -> Result<C, Error> {
        // Fetch thread should never be blocked for a long time in receiving buffers. Yet it could
        // wait for a long time on the application logic to receive an arrow buffer using next. We
        // drop the receiver here explicitly in order to be always able to join the fetch thread,
        // even if the iterator has not been consumed to completion.
        drop(self.receive_batch);
        if let Some(cursor) = self.cursor {
            Ok(cursor)
        } else {
            self.fetch_thread.unwrap().join().unwrap()
        }
    }
}

impl<C> ConcurrentBlockCursor<C> {
    /// Fetches values from the ODBC datasource into buffer. Values are streamed batch by batch in
    /// order to avoid reallocation of the buffers used for tranistion.
    /// 
    /// # Parameters
    /// 
    /// * `buffer`: A columnar any buffer which can bind to the cursor wrapped by this instance.
    ///   After the method call the reference will not point to the same instance which had been
    ///   passed into the function call, but to the one which was bound to the cursor in order to
    ///   fetch the last batch. The buffer passed into this method, is then used to fetch the next
    ///   batch. As such this method is ideal to implement concurrent fetching using two buffers.
    ///   One which is written to, and one that is read, which flip their roles between batches.
    ///   Also called double buffering.
    /// 
    /// # Return
    /// 
    /// * `true`: Fetched a batch from the data source. The contents of that batch are now in
    ///   `buffer`.
    /// * `false`: No batch could be fetched. The result set is consumed completly.
    pub fn fetch_into(&mut self, buffer: &mut ColumnarAnyBuffer) -> Result<bool, Error> {
        match self.receive_batch.recv() {
            // We successfully fetched a batch from the database.
            Ok(mut batch) => {
                swap(buffer, &mut batch);
                let _ = self.send_buffer.send(batch);
                Ok(true)
            }
            // Fetch thread stopped sending batches. Either because we consumed the result set
            // completly or we hit an error.
            Err(_receive_error) => {
                if let Some(join_handle) = self.fetch_thread.take() {
                    // If there has been an error returning the batch, or unbinding the buffer `?`
                    // will raise it.
                    self.cursor = Some(join_handle.join().unwrap()?);
                    // We ran out of batches in the result set. End the stream.
                    Ok(false)
                } else {
                    // This only happen if this method is called after it returned either `false` or
                    // `Err` once. Let us treat this scenario like a result set which is consumed
                    // completly.
                    Ok(false)
                }
            }
        }
    }
}
