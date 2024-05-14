from pybudget import get_spending, FileManager, LabellingAssistant

fm = FileManager()

# transactions = fm.get_transactions(start_date='04/01/2024')
# labeled_transactions = run_interactive_labeling_task(transactions)
# 
# fm.update_transactions(labeled_transactions)
# fm.save()

transactions = fm.get_transactions()

labeled_transactions = transactions.loc[transactions['category'] != 'TO_LABEL']
labels = set()


la = LabellingAssistant()
la.train_category_model(labeled_transactions)

for t in labeled_transactions.itertuples():
    t_labels = t.category.split(',')
    for tl in t_labels:
        labels.add(tl)

unlabeled_transactions = transactions.loc[transactions['category'] == 'TO_LABEL']

la.label_transactions(unlabeled_transactions, list(labels))