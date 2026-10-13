//! Typed filters and sorts.
//!
//! A [`Filter`] is a tree of conditions on the [`Field`]s of one index, and
//! a [`Sort`] names a field and an [`Order`]. Both deserialize from client
//! JSON and can only name the fields their index allows:
//!
//! ```json
//! {"and": [{"eq": ["author", 7]}, {"not": {"in": ["status", ["draft", "deleted"]]}}]}
//! ```
//!
//! See the [search guide](https://www.wakuwaku.rs/en/docs/data/meilisearch/search).

use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Write as _};

/// Deepest nesting of groups and negations a filter may have when rendered.
///
/// Meilisearch rejects filters nested deeper than 150 levels. This lower
/// bound also keeps rendering a deserialized filter cheap.
pub const MAX_DEPTH: usize = 64;

/// An attribute of an index, used in filters or in sorts.
///
/// Implement it on a fieldless enum. Derive `Deserialize` too, to accept
/// the field from clients.
pub trait Field: Copy + Debug + Send + Sync + 'static {
    /// Every field of the set.
    const ALL: &'static [Self];

    /// Name of the attribute in the documents. Use dots for nested
    /// attributes (`author.id`).
    fn name(self) -> &'static str;
}

/// A value compared against an attribute.
///
/// Deserializes from a JSON boolean, number or string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    /// A boolean, matched as the string `true` or `false`.
    Bool(bool),
    /// A signed integer.
    Int(i64),
    /// An unsigned integer too large for [`Value::Int`].
    UInt(u64),
    /// A float. Rendering fails if it is NaN or infinite.
    Float(f64),
    /// A string.
    Text(String),
}

macro_rules! value_from {
    ($variant:ident: $($ty:ty),+) => {
        $(
            impl From<$ty> for Value {
                fn from(value: $ty) -> Self {
                    Self::$variant(value.into())
                }
            }
        )+
    };
}

value_from!(Bool: bool);
value_from!(Int: i8, i16, i32, i64, u8, u16, u32);
value_from!(UInt: u64);
value_from!(Float: f32, f64);
value_from!(Text: String, &str);

/// A condition on the documents of an index whose filterable fields are
/// `F`.
///
/// Serialized with external tags: `{"eq": ["author", 7]}`,
/// `{"exists": "author"}`, `{"and": [...]}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Filter<F> {
    /// The attribute equals the value. For an array attribute, one of its
    /// elements does. Strings compare case-insensitively.
    Eq(F, Value),
    /// The documents [`Eq`](Self::Eq) doesn't select.
    Ne(F, Value),
    /// The attribute is greater than the value.
    Gt(F, Value),
    /// The attribute is greater than or equal to the value.
    Gte(F, Value),
    /// The attribute is less than the value.
    Lt(F, Value),
    /// The attribute is less than or equal to the value.
    Lte(F, Value),
    /// The attribute lies between the two values, both included.
    Between(F, Value, Value),
    /// The attribute equals one of the values. Matches nothing if there
    /// are none.
    In(F, Vec<Value>),
    /// The document has the attribute, even if it is `null` or empty.
    Exists(F),
    /// The attribute is `null`.
    IsNull(F),
    /// The attribute is `""`, `[]` or `{}`.
    IsEmpty(F),
    /// The condition doesn't hold.
    Not(Box<Self>),
    /// Every condition holds. Rendering fails if there are none.
    And(Vec<Self>),
    /// At least one condition holds. Rendering fails if there are none.
    Or(Vec<Self>),
    /// A filter expression in Meilisearch's syntax, rendered as is.
    ///
    /// # Danger
    ///
    /// The expression is trusted completely:
    ///
    /// - Never build it from untrusted input: a value spliced into it can
    ///   change the structure of the expression.
    /// - Parentheses don't contain it. `AND` binds tighter than `OR`, so the
    ///   raw text `a = 1) OR (b = 2`, rendered inside `(scope) AND (…)`,
    ///   matches `b = 2` outside the scope.
    /// - It may name any filterable attribute of the index, not only the
    ///   fields of `F`, and use `_foreign()`.
    ///
    /// It can't lift a scope sent as a separate filter (the scope of a
    /// [`Search`](super::search::Search) or of a tenant token), but a raw
    /// node inside a scope can make the scope match everything.
    ///
    /// Clients can't send it: it is skipped by serde.
    #[serde(skip)]
    DangerouslyRaw(String),
}

/// Why a [`Filter`] can't be rendered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FilterError {
    /// An [`And`](Filter::And) or [`Or`](Filter::Or) without conditions.
    #[error("a filter group has no conditions")]
    EmptyGroup,
    /// A [`Value::Float`] that is NaN or infinite.
    #[error("a filter value is NaN or infinite")]
    NonFiniteNumber,
    /// Groups and negations nested deeper than [`MAX_DEPTH`].
    #[error("the filter is nested deeper than {MAX_DEPTH} levels")]
    TooDeep,
}

impl<F: Field> Filter<F> {
    /// A [`Filter::DangerouslyRaw`] of `expression`. Read its danger notes
    /// first.
    pub fn dangerously_raw(expression: impl Into<String>) -> Self {
        Self::DangerouslyRaw(expression.into())
    }

    /// The filter as a Meilisearch filter expression.
    ///
    /// Every attribute and value is quoted, with `"` and `\` escaped, so no
    /// value can change the structure of the expression. Each member of a
    /// group and the operand of a negation is parenthesized.
    ///
    /// # Errors
    ///
    /// [`FilterError`] for an empty group, a non-finite float, or nesting
    /// deeper than [`MAX_DEPTH`].
    pub fn render(&self) -> Result<String, FilterError> {
        let mut out = String::new();
        self.write(&mut out, 0)?;
        Ok(out)
    }

    fn write(&self, out: &mut String, depth: usize) -> Result<(), FilterError> {
        if depth > MAX_DEPTH {
            return Err(FilterError::TooDeep);
        }
        match self {
            Self::Eq(field, value) => compare(out, *field, "=", value),
            Self::Ne(field, value) => compare(out, *field, "!=", value),
            Self::Gt(field, value) => compare(out, *field, ">", value),
            Self::Gte(field, value) => compare(out, *field, ">=", value),
            Self::Lt(field, value) => compare(out, *field, "<", value),
            Self::Lte(field, value) => compare(out, *field, "<=", value),
            Self::Between(field, low, high) => {
                quote(out, field.name());
                out.push(' ');
                write_value(out, low)?;
                out.push_str(" TO ");
                write_value(out, high)
            }
            Self::In(field, values) => {
                quote(out, field.name());
                out.push_str(" IN [");
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    write_value(out, value)?;
                }
                out.push(']');
                Ok(())
            }
            Self::Exists(field) => postfix(out, *field, " EXISTS"),
            Self::IsNull(field) => postfix(out, *field, " IS NULL"),
            Self::IsEmpty(field) => postfix(out, *field, " IS EMPTY"),
            Self::Not(filter) => {
                out.push_str("NOT ");
                filter.write_grouped(out, depth)
            }
            Self::And(filters) => write_group(out, filters, " AND ", depth),
            Self::Or(filters) => write_group(out, filters, " OR ", depth),
            Self::DangerouslyRaw(expression) => {
                out.push_str(expression);
                Ok(())
            }
        }
    }

    /// Write the filter in parentheses, one level deeper.
    fn write_grouped(&self, out: &mut String, depth: usize) -> Result<(), FilterError> {
        out.push('(');
        self.write(out, depth.saturating_add(1))?;
        out.push(')');
        Ok(())
    }
}

fn write_group<F: Field>(
    out: &mut String,
    filters: &[Filter<F>],
    operator: &str,
    depth: usize,
) -> Result<(), FilterError> {
    if filters.is_empty() {
        return Err(FilterError::EmptyGroup);
    }
    for (i, filter) in filters.iter().enumerate() {
        if i > 0 {
            out.push_str(operator);
        }
        filter.write_grouped(out, depth)?;
    }
    Ok(())
}

fn compare<F: Field>(
    out: &mut String,
    field: F,
    operator: &str,
    value: &Value,
) -> Result<(), FilterError> {
    quote(out, field.name());
    out.push(' ');
    out.push_str(operator);
    out.push(' ');
    write_value(out, value)
}

fn postfix<F: Field>(out: &mut String, field: F, operator: &str) -> Result<(), FilterError> {
    quote(out, field.name());
    out.push_str(operator);
    Ok(())
}

/// Write `value` as a quoted string. Meilisearch reads quoted numbers as
/// numbers, so every value goes through the same escaping.
fn write_value(out: &mut String, value: &Value) -> Result<(), FilterError> {
    match value {
        Value::Bool(value) => quote(out, if *value { "true" } else { "false" }),
        Value::Int(value) => quote_display(out, value),
        Value::UInt(value) => quote_display(out, value),
        Value::Float(value) if value.is_finite() => quote_display(out, value),
        Value::Float(_) => return Err(FilterError::NonFiniteNumber),
        Value::Text(value) => quote(out, value),
    }
    Ok(())
}

/// Write a number in quotes. Its digits, sign and point need no escaping.
fn quote_display(out: &mut String, value: &impl std::fmt::Display) {
    // Writing to a `String` can't fail.
    let _ = write!(out, "\"{value}\"");
}

/// Write `text` in double quotes, escaping `"` and `\` with a backslash.
///
/// Meilisearch unescapes both inside quotes, so any string round-trips,
/// and every quote in the output but the last is escaped: the value can't
/// end the string early.
fn quote(out: &mut String, text: &str) {
    out.reserve(text.len().saturating_add(2));
    out.push('"');
    for c in text.chars() {
        if matches!(c, '"' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    /// Smallest first.
    #[default]
    Asc,
    /// Largest first.
    Desc,
}

/// Sort on the sortable field `F`.
///
/// Deserializes from `{"field": "price", "order": "desc"}`; the order
/// defaults to ascending.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sort<F> {
    /// The attribute to sort on.
    pub field: F,
    /// The direction.
    #[serde(default)]
    pub order: Order,
}

impl<F: Field> Sort<F> {
    /// Sort on `field`, smallest first.
    pub const fn asc(field: F) -> Self {
        Self {
            field,
            order: Order::Asc,
        }
    }

    /// Sort on `field`, largest first.
    pub const fn desc(field: F) -> Self {
        Self {
            field,
            order: Order::Desc,
        }
    }

    /// The sort as Meilisearch expects it: `attribute:asc`.
    pub fn render(&self) -> String {
        let order = match self.order {
            Order::Asc => "asc",
            Order::Desc => "desc",
        };
        format!("{}:{order}", self.field.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Post {
        Author,
        Title,
    }

    impl Field for Post {
        const ALL: &'static [Self] = &[Self::Author, Self::Title];

        fn name(self) -> &'static str {
            match self {
                Self::Author => "author",
                Self::Title => "title",
            }
        }
    }

    #[test]
    fn values_that_look_like_syntax_stay_inside_their_quotes() {
        let filter = Filter::Eq(Post::Title, r#"x" OR author EXISTS OR title = "y"#.into());
        assert_eq!(
            filter.render(),
            Ok(r#""title" = "x\" OR author EXISTS OR title = \"y""#.to_owned())
        );

        // A trailing backslash would escape the closing quote if left alone.
        let filter = Filter::In(Post::Title, vec![r"a\".into(), r#"\"#.into()]);
        assert_eq!(
            filter.render(),
            Ok(r#""title" IN ["a\\", "\\"]"#.to_owned())
        );
    }

    #[test]
    fn groups_parenthesize_every_member() {
        let filter = Filter::And(vec![
            Filter::Or(vec![
                Filter::Eq(Post::Author, 1.into()),
                Filter::Exists(Post::Title),
            ]),
            Filter::Not(Box::new(Filter::Between(
                Post::Author,
                2.into(),
                2.5.into(),
            ))),
        ]);
        assert_eq!(
            filter.render(),
            Ok(
                r#"(("author" = "1") OR ("title" EXISTS)) AND (NOT ("author" "2" TO "2.5"))"#
                    .to_owned()
            )
        );
    }

    #[test]
    fn unrenderable_filters_are_rejected() {
        let empty: Filter<Post> = Filter::Or(vec![]);
        assert_eq!(empty.render(), Err(FilterError::EmptyGroup));

        let nan = Filter::Gt(Post::Author, f64::NAN.into());
        assert_eq!(nan.render(), Err(FilterError::NonFiniteNumber));

        let mut deep = Filter::Exists(Post::Author);
        for _ in 0..=MAX_DEPTH {
            deep = Filter::Not(Box::new(deep));
        }
        assert_eq!(deep.render(), Err(FilterError::TooDeep));
    }

    #[test]
    fn clients_can_send_typed_filters_but_not_raw_ones() {
        let filter: Filter<Post> =
            serde_json::from_str(r#"{"and": [{"eq": ["author", 7]}, {"exists": "title"}]}"#)
                .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            filter,
            Filter::And(vec![
                Filter::Eq(Post::Author, Value::Int(7)),
                Filter::Exists(Post::Title),
            ])
        );

        let raw = serde_json::from_str::<Filter<Post>>(r#"{"dangerously_raw": "author EXISTS"}"#);
        assert!(raw.is_err());
        let unknown_field = serde_json::from_str::<Filter<Post>>(r#"{"exists": "password"}"#);
        assert!(unknown_field.is_err());
    }
}
