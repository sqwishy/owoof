use std::fmt;

use crate::sql;

#[derive(Debug, Clone)]
pub struct Explanation {
    pub lines: Vec<ExplainLine>,
}

/// (From SQLite shell.c:explain_data_prepare ...)
///
/// The indenting rules are:
///
///     * For each "Next", "Prev", "VNext" or "VPrev" instruction, indent
///       all opcodes that occur between the p2 jump destination and the opcode
///       itself by 2 spaces.
///
///     * For each "Goto", if the jump destination is earlier in the program
///       and ends on one of:
///          Yield  SeekGt  SeekLt  RowSetRead  Rewind
///       or if the P1 parameter is one instead of zero,
///       then indent all opcodes between the earlier instruction
///       and "Goto" by 2 spaces.
impl fmt::Display for Explanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut indent = vec![];
        indent.resize_with(self.lines.len(), || 0u8);

        let mut yields = vec![];
        yields.resize_with(self.lines.len(), || false);

        let yield_codes = ["Yield", "SeekGt", "SeekLt", "RowSetRead", "Rewind"];
        let next_codes = ["Next", "Prev", "VNext", "VPref"];

        for (e, line) in self.lines.iter().enumerate() {
            let p2 = line.p2 as usize;

            match line.opcode.as_str() {
                op if yield_codes.contains(&op) => yields.get_mut(e).map(|y| *y = true),
                op if next_codes.contains(&op) => indent
                    .get_mut(p2..e)
                    .map(|slice| slice.iter_mut().for_each(|i| *i = *i + 1)),
                "Goto" if p2 < e && (yields.get(p2) == Some(&true) || line.p1 == 1) => indent
                    .get_mut(p2..e)
                    .map(|slice| slice.iter_mut().for_each(|i| *i = *i + 1)),
                _ => None,
            };
        }

        for (indent, line) in indent.into_iter().zip(self.lines.iter()) {
            writeln!(f, "{1:0$} {2}", indent as usize, "", line)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ExplainLine {
    pub addr: i64,
    pub opcode: String,
    pub p1: i64,
    pub p2: i64,
    pub p3: i64,
    pub p4: String,
    pub p5: String,
    pub comment: Option<String>,
}

impl ExplainLine {
    pub(crate) fn from_row_cursor(c: &mut sql::RowCursor) -> rusqlite::Result<Self> {
        Ok(ExplainLine {
            addr: c.get()?,
            opcode: c.get()?,
            p1: c.get()?,
            p2: c.get()?,
            p3: c.get()?,
            p4: c.get()?,
            p5: c.get()?,
            comment: c.get()?,
        })
    }
}

impl fmt::Display for ExplainLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{addr:>4}  {opcode:<16}  {p1:>4}  {p2:>4}  {p3:>4}  {p4:<12}  {p5:<22}  {comment}",
            addr = self.addr,
            opcode = self.opcode,
            p1 = self.p1,
            p2 = self.p2,
            p3 = self.p3,
            p4 = self.p4,
            p5 = self.p5,
            comment = self.comment.as_ref().map(String::as_str).unwrap_or(""),
        )
    }
}

#[derive(Debug, Clone)]
pub struct PlanExplanation {
    pub lines: Vec<PlanExplainLine>,
}

impl fmt::Display for PlanExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut indents = vec![0u8; self.lines.len()];

        for (e, line) in self.lines.iter().enumerate() {
            let indent = if line.parent > 0 {
                let search = self
                    .lines
                    .iter()
                    .zip(indents.iter())
                    .take(e)
                    .find(|(p, _)| p.id == line.parent)
                    .map(|(_, indent)| indent);
                if let Some(parent_indent) = search {
                    let indent = parent_indent + 1;
                    indents.get_mut(e).map(|i| *i = indent);
                    indent
                } else {
                    debug_assert!(false);
                    0
                }
            } else {
                0
            };

            writeln!(f, "{:<width$}-{}", "", line, width = indent as usize)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PlanExplainLine {
    pub id: i64,
    pub parent: i64,
    pub text: String,
}

impl PlanExplainLine {
    pub(crate) fn from_row_cursor(c: &mut sql::RowCursor) -> rusqlite::Result<Self> {
        Ok(PlanExplainLine {
            id: c.get()?,
            parent: c.get()?,
            text: c.skip(1).get()?,
        })
    }
}

impl fmt::Display for PlanExplainLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}
