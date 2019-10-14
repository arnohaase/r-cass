use std::io::ErrorKind;


pub (crate) fn other_error<T>(text: &str) -> std::io::Result<T> {
    Err(std::io::Error::new(ErrorKind::Other, text))
}
