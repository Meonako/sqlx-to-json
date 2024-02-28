use std::collections::HashMap;

use serde_json::Value as JsonValue;
use sqlx::{mysql::{MySqlValueRef, MySqlRow}, Row, Column, TypeInfo, Value, ValueRef};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

/// A wrapper for [`row_value_to_json`] function.
/// 
/// # Example
/// ```
/// use std::collections::HashMap;
/// use sqlx::{Row, Column};
///
/// let rows = sqlx::query("SELECT * FROM users LIMIT 10").fetch_all(&mut conn).await.unwrap();
/// let output = sqlx_to_json::postgres::rows_to_json(rows).unwrap();
/// ```
/// [`row_value_to_json`]: fn.row_value_to_json.html
pub fn rows_to_json(rows: Vec<MySqlRow>) -> Result<Vec<HashMap<String, JsonValue>>, String> {
    if rows.is_empty() {
        return Ok(vec![]);
    }

    let mut output = Vec::with_capacity(rows.len());

    for row in rows {
        let mut map = HashMap::new();

        for (i, column) in row.columns().iter().enumerate() {
            let row_value = row.try_get_raw(i).map_err(|e| e.to_string())?;
            let value_json = row_value_to_json(row_value).map_err(|e| e.to_string())?;
            map.insert(column.name().to_string(), value_json);
        }

        output.push(map);
    }

    Ok(output)
}

/// # Example
/// ```
/// use serde_json::Value;
/// use std::collections::HashMap;
/// 
/// let rows = sqlx::query("SELECT * FROM users LIMIT 10").fetch_all(&mut conn).await.unwrap();
/// let mut output = vec![];
/// 
/// for row in rows {
///     let mut map = HashMap::default();
/// 
///     for (i, column) in row.columns().iter().enumerate() {
///         let row_value = row.try_get_raw(i).unwrap();
///         let value_json = sqlx_to_json::mysql::to_json(row_value).unwrap();
///         map.insert(column.name().to_string(), value_json);
///     }
///
///     output.push(map);
/// }
/// ```
pub fn row_value_to_json(row_value: MySqlValueRef) -> Result<JsonValue, String> {
    if row_value.is_null() {
        return Ok(JsonValue::Null);
    }

    let res = match row_value.type_info().name() {
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode() {
                JsonValue::String(v)
            } else {
                JsonValue::Null
            }
        }
        "FLOAT" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<f32>() {
                JsonValue::from(v)
            } else {
                JsonValue::Null
            }
        }
        "DOUBLE" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<f64>() {
                JsonValue::from(v)
            } else {
                JsonValue::Null
            }
        }
        "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" | "BIGINT" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<i64>() {
                JsonValue::Number(v.into())
            } else {
                JsonValue::Null
            }
        }
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "INT UNSIGNED" | "MEDIUMINT UNSIGNED"
        | "BIGINT UNSIGNED" | "YEAR" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<u64>() {
                JsonValue::Number(v.into())
            } else {
                JsonValue::Null
            }
        }
        "BOOLEAN" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode() {
                JsonValue::Bool(v)
            } else {
                JsonValue::Null
            }
        }
        "DATE" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<Date>() {
                JsonValue::String(v.to_string())
            } else {
                JsonValue::Null
            }
        }
        "TIME" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<Time>() {
                JsonValue::String(v.to_string())
            } else {
                JsonValue::Null
            }
        }
        "DATETIME" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<PrimitiveDateTime>() {
                JsonValue::String(v.to_string())
            } else {
                JsonValue::Null
            }
        }
        "TIMESTAMP" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<OffsetDateTime>() {
                JsonValue::String(v.to_string())
            } else {
                JsonValue::Null
            }
        }
        "JSON" => ValueRef::to_owned(&row_value).try_decode().unwrap_or_default(),
        "TINIYBLOB" | "MEDIUMBLOB" | "BLOB" | "LONGBLOB" => {
            if let Ok(v) = ValueRef::to_owned(&row_value).try_decode::<Vec<u8>>() {
                JsonValue::Array(v.into_iter().map(|n| JsonValue::Number(n.into())).collect())
            } else {
                JsonValue::Null
            }
        }
        "NULL" => JsonValue::Null,
        _ => return Err(format!("Unsupported type: {}", row_value.type_info().name())),
    };

    Ok(res)
}