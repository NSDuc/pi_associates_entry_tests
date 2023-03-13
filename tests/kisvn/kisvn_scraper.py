import pandas as pd

a = pd.DataFrame([(1, 'a'),
                  (2, 'b'),
                  (3, 'c'),
                  (4, 'd')], columns=['id', 'name'])
b = pd.DataFrame([(1, 'a'),
                  (2, 'b2'),
                  (3, 'c2'),
                  (5, 'd')], columns=['id', 'name'])

# b = pd.DataFrame([], columns=['id', 'name'])
b = pd.DataFrame([])
print(f'b is not None: {b is not None}')
print(f'b is empty: {b.empty}')

only_in_a = pd.merge(a, b, how='left', indicator=True).query('_merge=="left_only"')
only_in_a.drop(['_merge'], axis=1)

print(only_in_a.info())
print(f'index is {only_in_a.index}')
print(only_in_a)
