mod as_handle;
mod buffer;
mod diagnostics;
mod environment;
mod error;
mod logging;

pub use self::{as_handle::AsHandle, environment::Environment, error::Error};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
