use tokio_util::bytes::{Buf, BytesMut};

use super::ProtocolError;
use crate::cache::query::CacheableQuery;

/// Prepared statement stored in connection state
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub name: String,
    pub sql: String,
    pub parameter_count: usize,
    pub cacheable_query: Option<Box<CacheableQuery>>,
}

/// Portal (bound prepared statement) stored in connection state
#[derive(Debug, Clone)]
pub struct Portal {
    pub name: String,
    pub statement_name: String,
    pub parameter_values: Vec<Option<Vec<u8>>>,
    pub parameter_formats: Vec<i16>, // 0=text, 1=binary
    pub result_formats: Vec<i16>,
}

impl Portal {
    /// Check if any parameter uses binary format (format code 1).
    /// Returns true if binary format is detected, false otherwise.
    pub fn has_binary_parameters(&self) -> bool {
        self.parameter_formats.contains(&1)
    }
}

/// Parsed Parse message data
#[derive(Debug, Clone)]
pub struct ParsedParseMessage {
    pub statement_name: String,
    pub sql: String,
    pub parameter_oids: Vec<u32>,
}

/// Parsed Bind message data
#[derive(Debug, Clone)]
pub struct ParsedBindMessage {
    pub portal_name: String,
    pub statement_name: String,
    pub parameter_formats: Vec<i16>,
    pub parameter_values: Vec<Option<Vec<u8>>>,
    pub result_formats: Vec<i16>,
}

/// Parsed Execute message data
#[derive(Debug, Clone)]
pub struct ParsedExecuteMessage {
    pub portal_name: String,
    pub max_rows: i32,
}

/// Parsed Describe message data
#[derive(Debug, Clone)]
pub struct ParsedDescribeMessage {
    pub describe_type: u8, // b'S' for statement, b'P' for portal
    pub name: String,
}

/// Parsed Close message data
#[derive(Debug, Clone)]
pub struct ParsedCloseMessage {
    pub close_type: u8, // b'S' for statement, b'P' for portal
    pub name: String,
}

/// Read a null-terminated string from the buffer
fn read_cstring<'a>(buf: &mut &'a [u8]) -> Result<&'a str, ProtocolError> {
    let null_pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing null terminator",
        )))?;

    let bytes = &buf[..null_pos];
    let s = std::str::from_utf8(bytes).map_err(|_| {
        ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid UTF-8 in string",
        ))
    })?;

    buf.advance(null_pos + 1);
    Ok(s)
}

/// Parse a Parse message ('P')
///
/// Format:
/// Byte1('P')
/// Int32 - message length
/// String - statement name (empty string for unnamed)
/// String - SQL query
/// Int16 - number of parameter data types
/// For each parameter:
///     Int32 - OID of parameter data type (0 = unspecified)
pub fn parse_parse_message(data: &BytesMut) -> Result<ParsedParseMessage, ProtocolError> {
    if data.len() < 5 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Parse message too short",
        )));
    }

    let mut buf = &data[5..]; // Skip message tag (1 byte) and length (4 bytes)

    let statement_name = read_cstring(&mut buf)?.to_owned();
    let sql = read_cstring(&mut buf)?.to_owned();

    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing parameter count",
        )));
    }

    let param_count = buf.get_i16() as usize;
    let mut parameter_oids = Vec::with_capacity(param_count);

    for _ in 0..param_count {
        if buf.len() < 4 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing parameter OID",
            )));
        }
        parameter_oids.push(buf.get_u32());
    }

    Ok(ParsedParseMessage {
        statement_name,
        sql,
        parameter_oids,
    })
}

/// Parse a Bind message ('B')
///
/// Format:
/// Byte1('B')
/// Int32 - message length
/// String - portal name (empty string for unnamed)
/// String - statement name
/// Int16 - number of parameter format codes
/// For each format code:
///     Int16 - format code (0=text, 1=binary)
/// Int16 - number of parameter values
/// For each parameter:
///     Int32 - parameter length (-1 = NULL)
///     Byte[n] - parameter value
/// Int16 - number of result column format codes
/// For each format code:
///     Int16 - format code (0=text, 1=binary)
pub fn parse_bind_message(data: &BytesMut) -> Result<ParsedBindMessage, ProtocolError> {
    if data.len() < 5 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Bind message too short",
        )));
    }

    let mut buf = &data[5..]; // Skip message tag and length

    let portal_name = read_cstring(&mut buf)?.to_owned();
    let statement_name = read_cstring(&mut buf)?.to_owned();

    // Read parameter format codes
    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing format code count",
        )));
    }
    let format_code_count = buf.get_i16() as usize;
    let mut parameter_formats = Vec::with_capacity(format_code_count);

    for _ in 0..format_code_count {
        if buf.len() < 2 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing format code",
            )));
        }
        parameter_formats.push(buf.get_i16());
    }

    // Read parameter values
    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing parameter value count",
        )));
    }
    let param_value_count = buf.get_i16() as usize;
    let mut parameter_values = Vec::with_capacity(param_value_count);

    for _ in 0..param_value_count {
        if buf.len() < 4 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing parameter length",
            )));
        }
        let param_len = buf.get_i32();

        if param_len == -1 {
            // NULL value
            parameter_values.push(None);
        } else {
            let param_len = param_len as usize;
            if buf.len() < param_len {
                return Err(ProtocolError::IoError(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "parameter value truncated",
                )));
            }
            let value = buf[..param_len].to_vec();
            buf.advance(param_len);
            parameter_values.push(Some(value));
        }
    }

    // Read result format codes
    if buf.len() < 2 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing result format code count",
        )));
    }
    let result_format_count = buf.get_i16() as usize;
    let mut result_formats = Vec::with_capacity(result_format_count);

    for _ in 0..result_format_count {
        if buf.len() < 2 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "missing result format code",
            )));
        }
        result_formats.push(buf.get_i16());
    }

    Ok(ParsedBindMessage {
        portal_name,
        statement_name,
        parameter_formats,
        parameter_values,
        result_formats,
    })
}

/// Parse an Execute message ('E')
///
/// Format:
/// Byte1('E')
/// Int32 - message length
/// String - portal name (empty string for unnamed)
/// Int32 - maximum number of rows to return (0 = unlimited)
pub fn parse_execute_message(data: &BytesMut) -> Result<ParsedExecuteMessage, ProtocolError> {
    if data.len() < 5 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Execute message too short",
        )));
    }

    let mut buf = &data[5..]; // Skip message tag and length

    let portal_name = read_cstring(&mut buf)?.to_owned();

    if buf.len() < 4 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing max_rows",
        )));
    }
    let max_rows = buf.get_i32();

    Ok(ParsedExecuteMessage {
        portal_name,
        max_rows,
    })
}

/// Parse a Describe message ('D')
///
/// Format:
/// Byte1('D')
/// Int32 - message length
/// Byte1 - 'S' for statement, 'P' for portal
/// String - name of statement or portal
pub fn parse_describe_message(data: &BytesMut) -> Result<ParsedDescribeMessage, ProtocolError> {
    if data.len() < 6 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Describe message too short",
        )));
    }

    let mut buf = &data[5..]; // Skip message tag and length

    let describe_type = buf.get_u8();
    let name = read_cstring(&mut buf)?.to_owned();

    Ok(ParsedDescribeMessage {
        describe_type,
        name,
    })
}

/// Parse a Close message ('C')
///
/// Format:
/// Byte1('C')
/// Int32 - message length
/// Byte1 - 'S' for statement, 'P' for portal
/// String - name of statement or portal
pub fn parse_close_message(data: &BytesMut) -> Result<ParsedCloseMessage, ProtocolError> {
    if data.len() < 6 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Close message too short",
        )));
    }

    let mut buf = &data[5..]; // Skip message tag and length

    let close_type = buf.get_u8();
    let name = read_cstring(&mut buf)?.to_owned();

    Ok(ParsedCloseMessage { close_type, name })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_parse_message() {
        // Parse message: statement name "stmt1", SQL "SELECT 1", no parameters
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P"); // tag
        data.extend_from_slice(&[0, 0, 0, 20]); // length (21 bytes total - 1 for tag = 20)
        data.extend_from_slice(b"stmt1\0"); // statement name
        data.extend_from_slice(b"SELECT 1\0"); // SQL
        data.extend_from_slice(&[0, 0]); // 0 parameters

        let result = parse_parse_message(&data).unwrap();
        assert_eq!(result.statement_name, "stmt1");
        assert_eq!(result.sql, "SELECT 1");
        assert_eq!(result.parameter_oids.len(), 0);
    }

    #[test]
    fn test_parse_parse_message_with_params() {
        // Parse message with 2 parameters
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 33]); // length
        data.extend_from_slice(b"\0"); // unnamed statement
        data.extend_from_slice(b"SELECT $1, $2\0"); // SQL
        data.extend_from_slice(&[0, 2]); // 2 parameters
        data.extend_from_slice(&[0, 0, 0, 23]); // OID 23 (int4)
        data.extend_from_slice(&[0, 0, 0, 25]); // OID 25 (text)

        let result = parse_parse_message(&data).unwrap();
        assert_eq!(result.statement_name, "");
        assert_eq!(result.sql, "SELECT $1, $2");
        assert_eq!(result.parameter_oids, vec![23, 25]);
    }

    #[test]
    fn test_parse_bind_message() {
        // Bind message: portal "p1", statement "s1", 1 text param "42", text result
        let mut data = BytesMut::new();
        data.extend_from_slice(b"B"); // tag
        data.extend_from_slice(&[0, 0, 0, 22]); // length
        data.extend_from_slice(b"p1\0"); // portal name
        data.extend_from_slice(b"s1\0"); // statement name
        data.extend_from_slice(&[0, 1]); // 1 format code
        data.extend_from_slice(&[0, 0]); // format 0 (text)
        data.extend_from_slice(&[0, 1]); // 1 parameter value
        data.extend_from_slice(&[0, 0, 0, 2]); // length 2
        data.extend_from_slice(b"42"); // value "42"
        data.extend_from_slice(&[0, 1]); // 1 result format code
        data.extend_from_slice(&[0, 0]); // format 0 (text)

        let result = parse_bind_message(&data).unwrap();
        assert_eq!(result.portal_name, "p1");
        assert_eq!(result.statement_name, "s1");
        assert_eq!(result.parameter_formats, vec![0]);
        assert_eq!(result.parameter_values.len(), 1);
        assert_eq!(result.parameter_values[0], Some(b"42".to_vec()));
        assert_eq!(result.result_formats, vec![0]);
    }

    #[test]
    fn test_parse_bind_message_with_null() {
        // Bind message with NULL parameter
        let mut data = BytesMut::new();
        data.extend_from_slice(b"B");
        data.extend_from_slice(&[0, 0, 0, 18]); // length
        data.extend_from_slice(b"\0"); // unnamed portal
        data.extend_from_slice(b"\0"); // unnamed statement
        data.extend_from_slice(&[0, 0]); // 0 format codes (use default text)
        data.extend_from_slice(&[0, 1]); // 1 parameter value
        data.extend_from_slice(&[255, 255, 255, 255]); // length -1 (NULL)
        data.extend_from_slice(&[0, 0]); // 0 result format codes

        let result = parse_bind_message(&data).unwrap();
        assert_eq!(result.portal_name, "");
        assert_eq!(result.statement_name, "");
        assert_eq!(result.parameter_values.len(), 1);
        assert_eq!(result.parameter_values[0], None);
    }

    #[test]
    fn test_parse_execute_message() {
        // Execute message: portal "p1", max_rows 100
        let mut data = BytesMut::new();
        data.extend_from_slice(b"E");
        data.extend_from_slice(&[0, 0, 0, 11]); // length
        data.extend_from_slice(b"p1\0"); // portal name
        data.extend_from_slice(&[0, 0, 0, 100]); // max_rows = 100

        let result = parse_execute_message(&data).unwrap();
        assert_eq!(result.portal_name, "p1");
        assert_eq!(result.max_rows, 100);
    }

    #[test]
    fn test_parse_execute_message_unlimited() {
        // Execute message: unnamed portal, unlimited rows
        let mut data = BytesMut::new();
        data.extend_from_slice(b"E");
        data.extend_from_slice(&[0, 0, 0, 9]); // length
        data.extend_from_slice(b"\0"); // unnamed portal
        data.extend_from_slice(&[0, 0, 0, 0]); // max_rows = 0 (unlimited)

        let result = parse_execute_message(&data).unwrap();
        assert_eq!(result.portal_name, "");
        assert_eq!(result.max_rows, 0);
    }

    #[test]
    fn test_parse_describe_message_statement() {
        // Describe statement "stmt1"
        let mut data = BytesMut::new();
        data.extend_from_slice(b"D");
        data.extend_from_slice(&[0, 0, 0, 11]); // length
        data.extend_from_slice(b"S"); // describe statement
        data.extend_from_slice(b"stmt1\0"); // name

        let result = parse_describe_message(&data).unwrap();
        assert_eq!(result.describe_type, b'S');
        assert_eq!(result.name, "stmt1");
    }

    #[test]
    fn test_parse_describe_message_portal() {
        // Describe portal "p1"
        let mut data = BytesMut::new();
        data.extend_from_slice(b"D");
        data.extend_from_slice(&[0, 0, 0, 8]); // length
        data.extend_from_slice(b"P"); // describe portal
        data.extend_from_slice(b"p1\0"); // name

        let result = parse_describe_message(&data).unwrap();
        assert_eq!(result.describe_type, b'P');
        assert_eq!(result.name, "p1");
    }

    #[test]
    fn test_parse_close_message_statement() {
        // Close statement "stmt1"
        let mut data = BytesMut::new();
        data.extend_from_slice(b"C");
        data.extend_from_slice(&[0, 0, 0, 11]); // length
        data.extend_from_slice(b"S"); // close statement
        data.extend_from_slice(b"stmt1\0"); // name

        let result = parse_close_message(&data).unwrap();
        assert_eq!(result.close_type, b'S');
        assert_eq!(result.name, "stmt1");
    }

    #[test]
    fn test_parse_close_message_portal() {
        // Close portal "p1"
        let mut data = BytesMut::new();
        data.extend_from_slice(b"C");
        data.extend_from_slice(&[0, 0, 0, 8]); // length
        data.extend_from_slice(b"P"); // close portal
        data.extend_from_slice(b"p1\0"); // name

        let result = parse_close_message(&data).unwrap();
        assert_eq!(result.close_type, b'P');
        assert_eq!(result.name, "p1");
    }

    #[test]
    fn test_parse_parse_message_truncated() {
        // Truncated Parse message
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 10]);

        let result = parse_parse_message(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_cacheable_select() {
        // Simple SELECT with WHERE clause - should be cacheable
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 50]); // length
        data.extend_from_slice(b"stmt1\0"); // statement name
        data.extend_from_slice(b"SELECT id, data FROM test WHERE id = $1\0"); // SQL
        data.extend_from_slice(&[0, 1]); // 1 parameter
        data.extend_from_slice(&[0, 0, 0, 23]); // OID 23 (int4)

        let result = parse_parse_message(&data).unwrap();
        assert_eq!(result.statement_name, "stmt1");
        assert_eq!(result.sql, "SELECT id, data FROM test WHERE id = $1");
        assert_eq!(result.parameter_oids, vec![23]);

        // Test that this SQL would be cacheable
        use crate::cache::query::CacheableQuery;
        use crate::query::ast::sql_query_convert;

        let ast = pg_query::parse(&result.sql).unwrap();
        let query = sql_query_convert(&ast).unwrap();
        let cacheable = CacheableQuery::try_from(&query);
        assert!(
            cacheable.is_ok(),
            "Simple SELECT with equality should be cacheable"
        );
    }

    #[test]
    fn test_parse_non_cacheable_subquery() {
        // SELECT with subquery - not currently cacheable
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 80]); // length
        data.extend_from_slice(b"\0"); // unnamed statement
        data.extend_from_slice(
            b"SELECT id FROM test WHERE id IN (SELECT id FROM other WHERE val = $1)\0",
        );
        data.extend_from_slice(&[0, 1]); // 1 parameter
        data.extend_from_slice(&[0, 0, 0, 25]); // OID 25 (text)

        let result = parse_parse_message(&data).unwrap();
        assert_eq!(result.statement_name, "");

        // Test that this SQL would NOT be cacheable (has subquery)
        // Subqueries fail during AST conversion (WhereParseError::UnsupportedPattern)
        use crate::query::ast::sql_query_convert;

        let ast = pg_query::parse(&result.sql).unwrap();
        let query_result = sql_query_convert(&ast);
        assert!(
            query_result.is_err(),
            "SELECT with subquery should not convert to AST"
        );
    }

    #[test]
    fn test_parse_cacheable_insert() {
        // INSERT statement - not a SELECT, should not be cacheable
        let mut data = BytesMut::new();
        data.extend_from_slice(b"P");
        data.extend_from_slice(&[0, 0, 0, 50]); // length
        data.extend_from_slice(b"\0"); // unnamed
        data.extend_from_slice(b"INSERT INTO test (id, data) VALUES ($1, $2)\0");
        data.extend_from_slice(&[0, 2]); // 2 parameters
        data.extend_from_slice(&[0, 0, 0, 23]); // OID 23 (int4)
        data.extend_from_slice(&[0, 0, 0, 25]); // OID 25 (text)

        let result = parse_parse_message(&data).unwrap();

        // Test that INSERT is not cacheable (not a SELECT)
        use crate::query::ast::sql_query_convert;

        let ast = pg_query::parse(&result.sql).unwrap();
        let cacheable_result = sql_query_convert(&ast);
        assert!(
            cacheable_result.is_err(),
            "INSERT should not convert to cacheable query"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_all_text() {
        let portal = Portal {
            name: "p1".to_string(),
            statement_name: "s1".to_string(),
            parameter_values: vec![Some(b"42".to_vec())],
            parameter_formats: vec![0], // text format
            result_formats: vec![0],
        };

        assert!(
            !portal.has_binary_parameters(),
            "All text parameters should return false"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_with_binary() {
        let portal = Portal {
            name: "p1".to_string(),
            statement_name: "s1".to_string(),
            parameter_values: vec![Some(vec![0, 0, 0, 42])],
            parameter_formats: vec![1], // binary format
            result_formats: vec![0],
        };

        assert!(
            portal.has_binary_parameters(),
            "Binary parameter should return true"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_mixed() {
        let portal = Portal {
            name: "p1".to_string(),
            statement_name: "s1".to_string(),
            parameter_values: vec![Some(b"text".to_vec()), Some(vec![0, 0, 0, 42])],
            parameter_formats: vec![0, 1], // text, then binary
            result_formats: vec![0],
        };

        assert!(
            portal.has_binary_parameters(),
            "Mixed formats with any binary should return true"
        );
    }

    #[test]
    fn test_portal_has_binary_parameters_empty() {
        let portal = Portal {
            name: "p1".to_string(),
            statement_name: "s1".to_string(),
            parameter_values: vec![],
            parameter_formats: vec![],
            result_formats: vec![],
        };

        assert!(
            !portal.has_binary_parameters(),
            "No parameters should return false"
        );
    }
}
