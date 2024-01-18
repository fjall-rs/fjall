use actix_web::{HttpResponse, ResponseError};

#[derive(Debug)]
pub struct MyError(fjall::Error);

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

impl From<fjall::Error> for MyError {
    fn from(value: fjall::Error) -> Self {
        Self(value)
    }
}

pub type RouteResult<T> = Result<T, MyError>;
