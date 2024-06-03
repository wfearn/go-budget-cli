from typing import Dict


def set_budget(budget: Dict[str, int]) -> Dict[str, int]:
    for category, budget_amount in budget.items():
        new_amount = input(f'What amount for {category} (current: {budget_amount})? ')
        budget[category] = int(new_amount) if new_amount else budget_amount

    return budget

