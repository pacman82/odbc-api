use std::{
    mem::swap,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

use crate::{BlockCursor, Cursor, Error};

use super::RowSetBuffer;

/// A wrapper around block cursors which fetches data in a dedicated system thread. Intended to
/// fetch data batch by batch while the application processes the batch last fetched. Works best
/// with a double buffer strategy using two fetch buffers.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{
///     Environment, buffers::{ColumnarAnyBuffer, BufferDesc}, Cursor, ConcurrentBlockCursor
/// };
/// use std::sync::OnceLock;
///
/// // We want to use the ODBC environment from another system thread without scope => Therefore it
/// // needs to be static.
/// static ENV: OnceLock<Environment> = OnceLock::new();
/// let env = Environment::new()?;
///
/// let conn = ENV.get_or_init(|| env).connect_with_connection_string(
///     "Driver={ODBC Driver 18 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;",
///     Default::default())?;
///
/// // We must use into_cursor to create a statement handle with static lifetime, which also owns
/// // the connection. This way we can send it to another thread safely.
/// let cursor = conn.into_cursor("SELECT * FROM very_big_table", ())?.unwrap();
///
/// // Batch size and buffer description. Here we assume there is only one integer column
/// let buffer_a = ColumnarAnyBuffer::from_descs(1000, [BufferDesc::I32 { nullable: false }]);
/// let mut buffer_b = ColumnarAnyBuffer::from_descs(1000, [BufferDesc::I32 { nullable: false }]);
/// // And now we have a sendable block cursor with static lifetime
/// let block_cursor = cursor.bind_buffer(buffer_a)?;
///
/// let mut cbc = ConcurrentBlockCursor::from_block_cursor(block_cursor);
/// while cbc.fetch_into(&mut buffer_b)? {
///     // Proccess batch in buffer b asynchronously to fetching it
/// }
///
/// # Ok::<_, odbc_api::Error>(())
/// ```
pub struct ConcurrentBlockCursor<C, B> {
    /// In order to avoid reallocating buffers over and over again, we use this channel to send the
    /// buffers back to the fetch thread after we copied their contents into arrow arrays.
    send_buffer: SyncSender<B>,
    /// Receives filled batches from the fetch thread. Once the source is empty or if an error
    /// occurs its associated sender is dropped, and receiving batches will return an error (which
    /// we expect during normal operation and cleanup, and is not forwarded to the user).
    receive_batch: Receiver<B>,
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

impl<C, B> ConcurrentBlockCursor<C, B>
where
    C: Cursor + Send + 'static,
    B: RowSetBuffer + Send + 'static,
{
    /// Construct a new concurrent block cursor.
    ///
    /// # Parameters
    ///
    /// * `block_cursor`: Taking a BlockCursor instead of a Cursor allows for better resource
    ///   stealing if constructing starting from a sequential Cursor, as we do not need to undbind
    ///   and bind the cursor.
    pub fn from_block_cursor(block_cursor: BlockCursor<C, B>) -> Self {
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

        Self {
            send_buffer,
            receive_batch,
            fetch_thread: Some(fetch_thread),
            cursor: None,
        }
    }

    /// Join fetch thread and yield the cursor back.
    pub fn into_cursor(self) -> Result<C, Error> {
        drop(self.receive_batch);
        // Dropping the send buffer is necessary to avoid deadlocks, in case there would not be any
        // buffer in the channel waiting for the fetch thread. Since we consume the cursor here, it
        // is also impossible for the application to send another buffer.
        drop(self.send_buffer);
        if let Some(cursor) = self.cursor {
            Ok(cursor)
        } else {
            self.fetch_thread.unwrap().join().unwrap()
        }
    }
}

impl<C, B> ConcurrentBlockCursor<C, B> {
    /// Receive the current batch and take ownership of its buffer. `None` if the cursor is already
    /// consumed, or had an error previously. This method blocks until a new batch available. In
    /// order for new batches available new buffers must be send to the thread in order for it to
    /// fill them. So calling fetch repeatedly without calling [`Self::fill`] in between may
    /// deadlock.
    pub fn fetch(&mut self) -> Result<Option<B>, Error> {
        match self.receive_batch.recv() {
            // We successfully fetched a batch from the database.
            Ok(batch) => Ok(Some(batch)),
            // Fetch thread stopped sending batches. Either because we consumed the result set
            // completly or we hit an error.
            Err(_receive_error) => {
                if let Some(join_handle) = self.fetch_thread.take() {
                    // If there has been an error returning the batch, or unbinding the buffer `?`
                    // will raise it.
                    self.cursor = Some(join_handle.join().unwrap()?);
                    // We ran out of batches in the result set. End the stream.
                    Ok(None)
                } else {
                    // This only happen if this method is called after it returned either `false` or
                    // `Err` once. Let us treat this scenario like a result set which is consumed
                    // completly.
                    Ok(None)
                }
            }
        }
    }

    /// Send a buffer to the thread fetching in order for it to be filled and to be retrieved later
    /// using either `fetch`, or `fetch_into`.
    pub fn fill(&mut self, buffer: B) {
        let _ = self.send_buffer.send(buffer);
    }

    /// Fetches values from the ODBC datasource into buffer. Values are streamed batch by batch in
    /// order to avoid reallocation of the buffers used for tranistion. This call blocks until a new
    /// batch is ready. This method combines both [`Self::fetch`] and [`Self::fill`].
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
    pub fn fetch_into(&mut self, buffer: &mut B) -> Result<bool, Error> {
        if let Some(mut batch) = self.fetch()? {
            swap(buffer, &mut batch);
            self.fill(batch);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
