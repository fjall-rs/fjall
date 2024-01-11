#[derive(Clone, Debug)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

use Either::{Left, Right};

impl<L, R> Either<L, R> {
    pub fn left(&self) -> &L {
        match self {
            Left(value) => value,
            Right(_) => panic!("Accessed Right on Left value"),
        }
    }

    pub fn right(&self) -> &R {
        match self {
            Right(value) => value,
            Left(_) => panic!("Accessed Left on Right value"),
        }
    }
}
