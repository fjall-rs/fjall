use actix_web::{HttpResponse, ResponseError};

#[derive(Debug)]
pub struct MyError(lsm_tree::Error);

impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl ResponseError for MyError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().body("Internal Server Error")
    }
}

impl From<lsm_tree::Error> for MyError {
    fn from(value: lsm_tree::Error) -> Self {
        Self(value)
    }
}

pub type MyResult<T> = Result<T, MyError>;
