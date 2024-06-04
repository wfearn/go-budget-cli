from typing import Dict


def set_budget(budget: Dict[str, int]) -> Dict[str, int]:
    for category, budget_amount in budget.items():
        new_amount = input(f'What amount for {category} (current: {budget_amount})? ')
        budget[category] = int(new_amount) if new_amount else budget_amount

    print()

    additional_category = 'category'

    while True:
        if not additional_category: break

        additional_category = input('What other category would you like to add? ')

        if additional_category in budget:
            print(f'\t{additional_category} already accounted for.')
        elif additional_category and additional_category not in budget:
            print()
            amount = int(input(f'How much would you like to allocate for {additional_category}? '))
            budget[additional_category] = amount

    print()
    return budget

