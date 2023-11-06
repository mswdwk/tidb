package hbase

// ExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type SessionAstStmtType int

// String defines a Stringer function for debugging and pretty printing.
func (k SessionAstStmtType) String() string {
	return "Session_ast_stmt_type"
}

// ExecStmtVarKey is a variable key for ExecStmt.
const SessionAstStmtWhereKey SessionAstStmtType = 0
