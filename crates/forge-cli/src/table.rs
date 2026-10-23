//! Prints a `/v1/sql` envelope (`{columns, rows, truncated, …}`) as a table or as JSON.
use serde_json::Value;

/// One cell: null is empty, strings unquoted, anything else compact JSON.
fn cell(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Header, a rule, fixed-width rows (padded by chars), then `(n rows)` and `(truncated)` when set.
pub fn render_table(env: &Value) -> String {
    let names: Vec<String> = env["columns"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| c["name"].as_str().unwrap_or_default().to_string())
        .collect();
    let rows: Vec<Vec<String>> = env["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|r| names.iter().map(|n| cell(&r[n.as_str()])).collect())
        .collect();
    let width = |i: usize| {
        rows.iter()
            .map(|r| r[i].chars().count())
            .chain([names[i].chars().count()])
            .max()
            .unwrap_or(0)
    };
    let widths: Vec<usize> = (0..names.len()).map(width).collect();
    let line = |cells: &[String]| {
        let padded: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(c, w)| format!("{c}{}", " ".repeat(w - c.chars().count())))
            .collect();
        padded.join(" | ").trim_end().to_string()
    };
    let mut out = String::new();
    if !names.is_empty() {
        out += &line(&names);
        out.push('\n');
        let rule: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
        out += &rule.join("-+-");
        out.push('\n');
    }
    for r in &rows {
        out += &line(r);
        out.push('\n');
    }
    let n = rows.len();
    out += &format!("({n} row{})\n", if n == 1 { "" } else { "s" });
    if env["truncated"] == Value::Bool(true) {
        out += "(truncated)\n";
    }
    out
}

pub fn render_json(env: &Value) -> String {
    serde_json::to_string_pretty(env).expect("a JSON value serializes") + "\n"
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env(rows: Value, truncated: bool) -> Value {
        json!({"columns": [{"name": "name", "type": "Utf8"}, {"name": "tags", "type": "List"}],
               "rows": rows, "row_count": 1, "truncated": truncated, "elapsed_ms": 1.0})
    }

    #[test]
    fn nulls_nested_and_truncated() {
        let t = render_table(&env(json!([{"name": null, "tags": ["a", {"b": 1}]}]), true));
        assert_eq!(
            t,
            "name | tags\n-----+--------------\n     | [\"a\",{\"b\":1}]\n(1 row)\n(truncated)\n"
        );
    }

    #[test]
    fn widths_count_chars_not_bytes() {
        let t = render_table(&env(
            json!([{"name": "héllo", "tags": null}, {"name": "x", "tags": []}]),
            false,
        ));
        let lines: Vec<&str> = t.lines().collect();
        assert_eq!(lines[0], "name  | tags");
        assert_eq!(lines[2], "héllo |");
        assert_eq!(lines[3], "x     | []");
        assert_eq!(lines[4], "(2 rows)");
        assert!(!t.contains("truncated"));
    }

    #[test]
    fn json_is_the_envelope() {
        let e = env(json!([]), false);
        assert_eq!(serde_json::from_str::<Value>(&render_json(&e)).unwrap(), e);
    }
}
