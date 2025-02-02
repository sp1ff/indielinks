// Copyright (C) 2025 Michael Herstine <sp1ff@pobox.com>
//
// This file is part of indielinks.
//
// indielinks is free software: you can redistribute it and/or modify it under the terms of the GNU
// General Public License as published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// indielinks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

//! # util
//!
//! Much as I loathe catch-all "utility" modules, I truly don't know where these belong. Hopefully,
//! as I build-out the project, this will become more clear.

use std::fmt::Display;

use either::Either;

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                          exactly_two                                           //
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ExactlyTwoError<T: std::iter::Iterator> {
    #[allow(clippy::type_complexity)]
    cause: Option<Either<T::Item, (T::Item, T::Item, T::Item)>>,
}

impl<T: std::iter::Iterator> Display for ExactlyTwoError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.cause {
            Some(either) => match either {
                Either::Left(_one) => write!(f, "ExactlyTwoError: One element"),
                Either::Right(_three) => write!(f, "ExactlyTwoError: Three or more elements"),
            },
            None => write!(f, "ExactlyTwoError: no elements"),
        }
    }
}

impl<T: std::iter::Iterator> ExactlyTwoError<T> {
    #[allow(clippy::type_complexity)]
    pub fn new(cause: Option<Either<T::Item, (T::Item, T::Item, T::Item)>>) -> ExactlyTwoError<T> {
        ExactlyTwoError { cause }
    }
}

pub fn exactly_two<T>(mut iter: T) -> std::result::Result<(T::Item, T::Item), ExactlyTwoError<T>>
where
    T: std::iter::Iterator,
{
    // sample code at https://docs.rs/itertools/latest/src/itertools/lib.rs.html#4050-4064
    match iter.next() {
        Some(first) => match iter.next() {
            Some(second) => match iter.next() {
                Some(third) => Err(ExactlyTwoError::<T>::new(Some(Either::Right((
                    first, second, third,
                ))))),
                None => Ok((first, second)),
            },
            None => Err(ExactlyTwoError::<T>::new(Some(Either::Left(first)))),
        },
        None => Err(ExactlyTwoError::<T>::new(None)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//                                           UpToThree                                            //
////////////////////////////////////////////////////////////////////////////////////////////////////

/// An enum expressing zero, one, two or three of a thing.
#[derive(Clone, Debug)]
pub enum UpToThree<T> {
    None,
    One(T),
    Two(T, T),
    Three(T, T, T),
}

#[derive(Debug)]
pub struct NoMoreThanThree {}

impl std::fmt::Display for NoMoreThanThree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoMoreThanThree")
    }
}

impl std::error::Error for NoMoreThanThree {}

impl<T: Clone> UpToThree<T> {
    pub fn new<U: IntoIterator<Item = T>>(
        iter: U,
    ) -> std::result::Result<UpToThree<T>, NoMoreThanThree> {
        let v = iter.into_iter().collect::<Vec<T>>();
        match v.len() {
            0 => Ok(UpToThree::None),
            1 => Ok(UpToThree::One(v[0].clone())),
            2 => Ok(UpToThree::Two(v[0].clone(), v[1].clone())),
            3 => Ok(UpToThree::Three(v[0].clone(), v[1].clone(), v[2].clone())),
            _ => Err(NoMoreThanThree {}),
        }
    }
}
