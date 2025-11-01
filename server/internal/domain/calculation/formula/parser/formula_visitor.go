// Code generated from Formula.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Formula
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by Formula.
type FormulaVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by Formula#root.
	VisitRoot(ctx *RootContext) interface{}

	// Visit a parse tree produced by Formula#FieldReferenceCurly.
	VisitFieldReferenceCurly(ctx *FieldReferenceCurlyContext) interface{}

	// Visit a parse tree produced by Formula#UnaryOp.
	VisitUnaryOp(ctx *UnaryOpContext) interface{}

	// Visit a parse tree produced by Formula#StringLiteral.
	VisitStringLiteral(ctx *StringLiteralContext) interface{}

	// Visit a parse tree produced by Formula#Brackets.
	VisitBrackets(ctx *BracketsContext) interface{}

	// Visit a parse tree produced by Formula#BooleanLiteral.
	VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{}

	// Visit a parse tree produced by Formula#RightWhitespaceOrComments.
	VisitRightWhitespaceOrComments(ctx *RightWhitespaceOrCommentsContext) interface{}

	// Visit a parse tree produced by Formula#DecimalLiteral.
	VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{}

	// Visit a parse tree produced by Formula#LeftWhitespaceOrComments.
	VisitLeftWhitespaceOrComments(ctx *LeftWhitespaceOrCommentsContext) interface{}

	// Visit a parse tree produced by Formula#FunctionCall.
	VisitFunctionCall(ctx *FunctionCallContext) interface{}

	// Visit a parse tree produced by Formula#IntegerLiteral.
	VisitIntegerLiteral(ctx *IntegerLiteralContext) interface{}

	// Visit a parse tree produced by Formula#BinaryOp.
	VisitBinaryOp(ctx *BinaryOpContext) interface{}

	// Visit a parse tree produced by Formula#ws_or_comment.
	VisitWs_or_comment(ctx *Ws_or_commentContext) interface{}

	// Visit a parse tree produced by Formula#field_reference.
	VisitField_reference(ctx *Field_referenceContext) interface{}

	// Visit a parse tree produced by Formula#field_reference_curly.
	VisitField_reference_curly(ctx *Field_reference_curlyContext) interface{}

	// Visit a parse tree produced by Formula#func_name.
	VisitFunc_name(ctx *Func_nameContext) interface{}

	// Visit a parse tree produced by Formula#identifier.
	VisitIdentifier(ctx *IdentifierContext) interface{}
}
