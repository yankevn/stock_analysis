from decimal import Decimal
from pprint import pprint
from typing import Any, Dict, Optional

from dynamodb import FinanceTermKeys, FinancialStatements

def retrieve_statement(
    stock_info: Dict[str, Any],
    financial_statement: FinancialStatements,
    year: str
) -> Optional[Any]:
    correct_statement = [statement for statement in stock_info[financial_statement.value] if statement[FinanceTermKeys.YEAR.value] == year]
    assert len(correct_statement) <= 1, f"Too many statements found: {correct_statement}"
    if correct_statement:
        return correct_statement[0]
    return None


def calculate_and_append_ratios(stock_info: Dict[str, Any]):
    year_to_capex = {}
    year_to_net_earnings = {}
    for ratios_statement in stock_info[FinancialStatements.RATIOS.value]:
        year = ratios_statement[FinanceTermKeys.YEAR.value]

        income_statement = retrieve_statement(stock_info, FinancialStatements.INCOME, year)
        balance_sheet_statement = retrieve_statement(stock_info, FinancialStatements.BALANCE_SHEET, year)
        cash_flow_statement = retrieve_statement(stock_info, FinancialStatements.CASH_FLOW, year)

        net_income = income_statement[FinanceTermKeys.NET_INCOME.value]
        long_term_debt = balance_sheet_statement[FinanceTermKeys.TOTAL_LONG_TERM_LIABILITIES.value]
        net_earnings_to_long_term_debt = net_income / long_term_debt
        ratios_statement[FinanceTermKeys.NET_EARNINGS_TO_LONG_TERM_DEBT.value] = net_earnings_to_long_term_debt

        shareholders_equity = balance_sheet_statement[FinanceTermKeys.SHAREHOLDERS_EQUITY.value]
        treasury_stock = balance_sheet_statement[FinanceTermKeys.TREASURY_STOCK.value]
        true_shareholders_equity = shareholders_equity + treasury_stock
        balance_sheet_statement[FinanceTermKeys.TRUE_SHAREHOLDERS_EQUITY.value] = true_shareholders_equity

        total_liabilities = balance_sheet_statement[FinanceTermKeys.TOTAL_LIABILITIES.value]
        adjusted_debt_to_true_shareholders_equity_ratio = total_liabilities / true_shareholders_equity
        ratios_statement[FinanceTermKeys.ADJUSTED_DEBT_TO_TRUE_SHAREHOLDERS_EQUITY_RATIO.value] = adjusted_debt_to_true_shareholders_equity_ratio

        adjusted_return_on_true_shareholders_equity_ratio = net_income / true_shareholders_equity
        ratios_statement[FinanceTermKeys.ADJUSTED_RETURN_ON_TRUE_SHAREHOLDERS_EQUITY_RATIO.value] = adjusted_debt_to_true_shareholders_equity_ratio

        pretax_income = income_statement[FinanceTermKeys.PRETAX_INCOME.value]
        shares_outstanding_diluted = income_statement[FinanceTermKeys.SHARES_OUTSTANDING_DILUTED.value]
        pretax_eps = pretax_income / shares_outstanding_diluted
        ratios_statement[FinanceTermKeys.PRETAX_EPS.value] = pretax_eps

        capital_expenditures = cash_flow_statement[FinanceTermKeys.CAPITAL_EXPENDITURES.value]
        year_to_capex[int(year)] = capital_expenditures
        year_to_net_earnings[int(year)] = net_income

    # Loop again to populate capex ratio
    for ratios_statement in stock_info[FinancialStatements.RATIOS.value]:
        year = int(ratios_statement[FinanceTermKeys.YEAR.value])

        # Calculate capex ratio for the past 5 years
        # Skip years that do not have 5 years of historical data
        if year - 5 + 1 in year_to_capex:
            capex_sum = Decimal(0)
            net_earnings_sum = Decimal(0)
            # Iterate through past 5 years, inclusive of current year
            # Ex. year = 2023 --> 2019, 2020, 2021, 2022, 2023
            for historical_year in range(year - 5 + 1, year + 1):
                capex_sum += year_to_capex[historical_year]
                net_earnings_sum += year_to_net_earnings[historical_year]

            capex_ratio = capex_sum / net_earnings_sum
            ratios_statement[FinanceTermKeys.CAPEX_RATIO.value] = capex_ratio

        else:
            # Remove the value
            ratios_statement.pop(FinanceTermKeys.CAPEX_RATIO.value, None)

    return stock_info
