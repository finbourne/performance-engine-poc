from misc import *


class ReturnSource():
    def __init__(self, **kwargs):
        dataset = kwargs.get('dataset')
        if type('dataset') is pd.DataFrame:
            self.data = dataset
        else:
            filename = kwargs.get('filename', 'test-data.xlsx')
            self.data = pd.read_excel(filename, sheet_name=dataset)

        if len(self.data.columns) != 4:
            portfolio = kwargs.get('portfolio', 'fund')
            self.data = self.data[['asat', 'date', f'ror.{portfolio}', f'wt.{portfolio}']]

        self.data.columns = ['asat', 'date', 'ror', 'wt']

        self.data['date'] = as_date(self.data['date'])
        self.data['asat'] = as_date(self.data['asat'])

    @as_dates
    def get_return_data(self, entity_scope, entity_code, from_date, to_date, asat):

        def check(r):
            return (r['date'] >= from_date and
                    r['date'] <= to_date and
                    r['asat'] <= asat)

        df = self.data[self.data.apply(check, axis=1)]
        df = df.drop('asat', axis=1).drop_duplicates('date', keep='last').sort_values('date')
        for idx, row in df.iterrows():
            yield row