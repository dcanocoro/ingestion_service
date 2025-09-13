from typing import List
from sqlalchemy.orm import Session
from ..models.income_statement import IncomeStatement
from ..schemas.income_statement import IncomeStatementIn


class IncomeStatementRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def upsert_many(self, statements: List[IncomeStatementIn]) -> None:
        for stmt in statements:
            existing = (
                self.db.query(IncomeStatement)
                .filter(
                    IncomeStatement.symbol == stmt.symbol,
                    IncomeStatement.fiscal_date_ending == stmt.fiscal_date_ending,
                )
                .one_or_none()
            )
            if existing:
                existing.total_revenue = stmt.total_revenue
                existing.gross_profit = stmt.gross_profit
                existing.operating_income = stmt.operating_income
                existing.ebit = stmt.ebit
                existing.ebitda = stmt.ebitda
                existing.net_income = stmt.net_income
            else:
                self.db.add(IncomeStatement(**stmt.dict()))
        self.db.commit()
