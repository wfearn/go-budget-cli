from pybudget import get_spending, FileManager, LabellingAssistant

fm = FileManager()
# transactions = fm.get_transactions(start_date='04/01/2024', end_date='04/30/2024')
# get_spending(transactions)
# fm.load_new_transactions()
# fm.save()
# 
# transactions = fm.get_transactions()
# 
# labeled_transactions = transactions.loc[transactions['category'] != 'TO_LABEL']
# labels = set()
# 
# la = LabellingAssistant()
# la.train_category_model(labeled_transactions)
# la.train_amount_model(labeled_transactions)
# 
# for t in labeled_transactions.itertuples():
#     t_labels = t.category.split(',')
#     for tl in t_labels:
#         labels.add(tl)
# 
# recent_transactions = fm.get_transactions(start_date='04/01/2024')
# unlabeled_transactions = transactions.loc[transactions['category'] == 'TO_LABEL']
# labeled_transactions = la.label_transactions(unlabeled_transactions, list(labels))
# 
# # NOTE: Uncomment this line when we're confident of system performance.
# fm.update_transactions(labeled_transactions)
# fm.save()