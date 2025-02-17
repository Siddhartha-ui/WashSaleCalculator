
import datetime
from operator import itemgetter

# This is a global value for the number of lots that have been created. It is
# global because we want to increment it whenever a Lot object is created,
# which is done in a number of different places.
_LOT_COUNT = 0


class Transaction(object):
    """Models a single lot of stock."""

    # A list of the field names for a Lot.
    FIELD_NAMES = ['num_shares', 'symbol', 'description', 'buy_date',
                   'adjusted_buy_date', 'basis', 'adjusted_basis', 'sell_date',
                   'proceeds', 'adjustment_code', 'adjustment',
                   'form_position', 'buy_lot', 'replacement_for',
                   'is_replacement', 'loss_processed' , 
                    'parent_lot', 'adjusted_sell_date', 'adjusted_proceeds' ]

    def __init__(self, num_shares, symbol, description, buy_date,
                 adjusted_buy_date, basis, adjusted_basis, sell_date, proceeds,
                 adjustment_code, adjustment, form_position, buy_lot,
                 replacement_for, is_replacement, loss_processed,parent_lot,adjusted_sell_date,adjusted_proceeds):
        """Initializes a lot.

        
        """
        self.num_shares = num_shares
        self.symbol = symbol
        self.description = description
        self.buy_date = buy_date
        self.adjusted_buy_date = adjusted_buy_date
        self.basis = basis
        self.adjusted_basis = adjusted_basis
        self.sell_date = sell_date
        self.proceeds = proceeds
        self.adjustment_code = adjustment_code
        self.adjustment = adjustment
        self.form_position = form_position
        self.buy_lot = buy_lot
        self.replacement_for = replacement_for
        self.is_replacement = is_replacement
        self.loss_processed = loss_processed
        self.parent_lot = parent_lot
        self.adjusted_sell_date = adjusted_sell_date
        self.adjusted_proceeds = adjusted_proceeds
        


        # The lot number is only used to sort otherwise equivalent lots.
        global _LOT_COUNT
        self._lot_number = _LOT_COUNT
        _LOT_COUNT += 1

                    

    def __getitem__(self, i):
        return i
    
    def get_buydate(self):
        return str(self.buy_date)

    def get_selldate(self):
        return str(self.sell_date)

    def get_lot(self):
        return self.buy_lot    
    
    def get_adjusted_buy_date(self) :
        return self.adjusted_buy_date

    def get_adjusted_sell_date(self) :
        return self.adjusted_sell_date

    def is_loss(self):
        """Determines whether this lot is a loss
        """

        """
        this section for non short selling
        """

        if self.sell_date != '' and self.buy_date != '' and self.sell_date >= self.buy_date :
               if  (self.proceeds + self.basis) < 0 :    
                  return True

        # if self.sell_date != '' and self.adjusted_buy_date != '' and self.sell_date >= self.adjusted_buy_date : 
        #     if (self.proceeds + self.adjusted_basis) < 0 :    
        #         return True     
        
        """
        this section for  short selling
        """

        if self.sell_date != '' and self.buy_date != '' and self.sell_date < self.buy_date :
               if  (self.proceeds + self.basis) < 0 :    
                  return True

        # if self.adjusted_sell_date != '' and self.buy_date!= '' and self.adjusted_sell_date < self.buy_date : 
        #     if (self.adjusted_proceeds + self.basis) < 0 :    
        #         return True    


        return False

    def is_buy(self):
        """Determines whether this lot is a sell.

        Returns:
            True if this lot was sold for a loss. False if it was sold for a
            gain, or it has not been sold.
        """
             
        if self.sell_date != ''  and self.buy_date == '' and self.num_shares > 0 :    
            return True
        return False

    def is_sell(self):
        """Determines whether this lot is a sell.

        Returns:
            True if this lot was sold for a loss. False if it was sold for a
            gain, or it has not been sold.
        """
             
        if self.sell_date != ''  and self.buy_date == '' and self.num_shares < 0 :    
            return True
        return False

    def __eq__(self, other):
        return (self.num_shares == other.num_shares and
                self.symbol == other.symbol and
                self.description == other.description and
                self.buy_date == other.buy_date and
                self.adjusted_buy_date == other.adjusted_buy_date and
                self.basis == other.basis and
                self.adjusted_basis == other.adjusted_basis and
                self.sell_date == other.sell_date and
                self.proceeds == other.proceeds and
                self.adjustment_code == other.adjustment_code and
                self.adjustment == other.adjustment and
                self.form_position == other.form_position and
                self.buy_lot == other.buy_lot and
                self.replacement_for == other.replacement_for and
                self.is_replacement == other.is_replacement and
                self.loss_processed == other.loss_processed and
                self.parent_lot == other.parent_lot and
                self.adjusted_sell_date == other.adjusted_sell_date and 
                self.adjusted_proceeds == other.adjusted_proceeds
                )

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return ' '.join(self.str_data())

    __repl__ = __str__

    def str_data(self):
        return ['{:d}'.format(self.num_shares),
                '{}'.format(self.symbol),
                '{}'.format(self.description),
                '{}'.format(self.buy_date),
                '{}'.format(self.adjusted_buy_date),
                '${:.2f}'.format(float(self.basis) / 100),
                '${:.2f}'.format(float(self.adjusted_basis) / 100),
                '{}'.format(self.sell_date),
                '${:.2f}'.format(float(self.proceeds) / 100),
                '{}'.format(self.adjustment_code),
                '${:.2f}'.format(float(self.adjustment) / 100),
                '{}'.format(self.form_position),
                '{}'.format(self.buy_lot),
                '{}'.format(','.join(self.replacement_for)),
                '{}'.format(self.is_replacement),
                '{}'.format(self.loss_processed),
                '{}'.format(self.parent_lot),
                '{}'.format(self.adjusted_sell_date),
                '${:.2f}'.format(float(self.adjusted_proceeds) / 100)]

    
    
    @staticmethod
    def sort_by_sell_date(self):
        return sorted(self , key= self.sell_date)
    
    @staticmethod
    def sort_by_buy_date(self):
        return sorted(self , key= self.buy_date)

class Transactions(object):
    """Contains a set of lots."""

    get_buy_key = "buy_date"
    get_sell_key = "sell_date"
    get_lot_key = "buy_lot"
    get_parent_lot = "parent_lot"
    get_symbol_key = "symbol"
    
    # A map of Lot field name to CSV header value.
    HEADERS = {
        'num_shares': 'Num Shares',
        'symbol': 'Symbol',
        'description': 'Description',
        'buy_date': 'Buy Date',
        'adjusted_buy_date': 'Adjusted Buy Date',
        'basis': 'Basis',
        'adjusted_basis': 'Adjusted Basis',
        'sell_date': 'Sell Date',
        'proceeds': 'Proceeds',
        'adjustment_code': 'Adjustment Code',
        'adjustment': 'Adjustment',
        'form_position': 'Form Position',
        'buy_lot': 'Buy Lot',
        'replacement_for': 'Replacement For',
        'is_replacement': 'Is Replacement',
        'loss_processed': 'Loss Processed',
        'parent_lot' : 'Parent Lot',
        'adjusted_sell_date' : 'Adjusted Sale Date',
        'adjusted_proceeds' : 'Adjusted Proceeds'
    }

    # A map of Lot field name to short strings naming the column.
    SHORT_HEADERS = {
        'num_shares': 'Num',
        'symbol': 'Symb',
        'description': 'Desc',
        'buy_date': 'BuyDate',
        'adjusted_buy_date': 'AdjBuyDate',
        'basis': 'Basis',
        'adjusted_basis': 'AdjBasis',
        'sell_date': 'Sell Date',
        'proceeds': 'Proceeds',
        'adjustment_code': 'AdjCode',
        'adjustment': 'Adj',
        'form_position': 'Pos',
        'buy_lot': 'BuyLot',
        'replacement_for': 'ReplFor',
        'is_replacement': 'IsRepl',
        'loss_processed': 'Processed',
        'parent_lot' : 'parentlot',
        'adjusted_sell_date' : 'AdjSaleDate',
        'adjusted_proceeds' : 'AdjProceeds'
    }

    def __init__(self, lots):
        """Creates a new set of lots.

        Populates the buy_lot field in each lot if it is not set.

        Args:
            lots: A list of Lot objects.
        """
        i = 1
        for lot in lots:
            if not lot.buy_lot:
                #lot.buy_lot = '_{}'.format(i)
                lot.buy_lot = str(i)
                i += 1
        self._lots = lots
        

    

    def lots(self):
        """Returns the list of Lot objects."""
        return self._lots

    def add(self, lot):
        """Adds a lot to this object.

        Args:
            lot: The Lot to add.
        """
        self._lots.append(lot)

    def remove(self,lot) :
        self._lots.remove(lot)

    def size(self):
        """Returns the number of lots."""
        return len(self._lots)

    def sort(self, **kwargs):
        self._lots.sort(**kwargs)

    def sorted_by_buy_date_and_id(self):
        return sorted(self._lots, key= self._lots.buy_date)

    def sorted_by_sale_date_and_id(self):
        return sorted(self._lots, key= self._lots.sell_date)

    def sorted(self):
        return  sorted(self._lots, key= self._lots.sell_date)

    def contents_equal(self, other):
        """Returns True if the individual lots are the same.

        This is different than __eq__ because the individual Lot objects do not
        need to have the same id(), just be equivalent.
        """
        for this, that in zip(self._lots, other._lots):
            if this != that:
                return False
        return True

    def __eq__(self, other):
        if len(self._lots) != len(other._lots):
            return False
        for lot in self._lots:
            if lot not in other._lots:
                return False
        return True

    def __ne__(self, other):
        return not self == other

    

    def __iter__(self):
        return iter(self._lots)

            
    @staticmethod
    def create_from_List_Object(data):
        """Creates a Lots object based on a multi-line string of csv data.

        The first line of the csv file must contain headers, which are the
        values of the HEADERS dict in order. All other lines should contain the
        values. See the test data for examples.

        Args:
            data: A list of strings, where each line is a CSV row that matches
                    the format above
        Returns:
            A Lots object
        """

        def convert_to_int(value):
            if value:
                return int(float(value))
            return 0

        def convert_to_float(value):
            if value:
                return float(value)
            return 0

   
        def convert_to_date(value):
            if value:
                return datetime.datetime.strptime(value, '%m/%d/%Y').date()
        
            return None

        def convert_to_bool(value):
            if value:
                return value.lower() == 'true'
            return False

        def convert_to_string_list(value):
            if value:
                return value.split('|')
            return []
        """
        FIELD_NAMES = ['num_shares', 'symbol', 'description', 'buy_date',
                   'adjusted_buy_date', 'basis', 'adjusted_basis', 'sell_date',
                   'proceeds', 'adjustment_code', 'adjustment',
                   'form_position', 'buy_lot', 'replacement_for',
                   'is_replacement', 'loss_processed' , 
                    'parent_lot', 'adjusted_sell_date', 'adjusted_proceeds' ]
        """
        
        lots = []
        for row in data:
            
            row['num_shares'] = convert_to_float(row['num_shares'])
            row['symbol'] = row['symbol']
            row['description'] = row['description']
            row['buy_date'] = convert_to_date(row['buy_date'])
            if not row['buy_date']:
                row['buy_date'] =''
            row['adjusted_buy_date'] = convert_to_date(row['adjusted_buy_date'])
            
            row['basis'] = convert_to_float(row['basis'])
            row['adjusted_basis'] = convert_to_float(row['adjusted_basis'])
         
            row['sell_date'] = convert_to_date(row['sell_date'])
            if not row['sell_date']:
                row['sell_date'] =''
            
            row['proceeds'] = convert_to_float(row['proceeds'])
            row['adjustment_code'] = row['adjustment_code']
            row['adjustment'] = convert_to_float(row['adjustment'])
            row['form_position'] = row['form_position']
            row['buy_lot'] = row['buy_lot']
            row['replacement_for'] = row['replacement_for']
            row['is_replacement'] = convert_to_bool(row['is_replacement'])
            row['loss_processed'] = convert_to_bool(row['loss_processed'])
            row['parent_lot'] = row['parent_lot']
            row['adjusted_sell_date'] = convert_to_date(row['adjusted_sell_date'])
            row['adjusted_proceeds'] = convert_to_float(row['adjusted_proceeds']) 
            
            lots.append(Transaction(**row))
        return Transactions(lots)
        


    def write_csv_data(self):
        """Writes this lots data as CSV data to an output file.

        Args:
            output_file: A file-like object to write to.
        """
        def convert_from_float(value):
            if value:
                return float(value)
            return 0

        def convert_from_int(value):
            if value:
                return str(value)
            return ''

        def convert_from_date(value):
            if value:
                return value.strftime('%m/%d/%Y')
            return ''

        def convert_from_bool(value):
            if value:
                return 'True'
            return ''

        def convert_from_string_list(value):
            if value:
                return '|'.join(value)
            return ''

        """
        FIELD_NAMES = ['num_shares', 'symbol', 'description', 'buy_date',
                   'adjusted_buy_date', 'basis', 'adjusted_basis', 'sell_date',
                   'proceeds', 'adjustment_code', 'adjustment',
                   'form_position', 'buy_lot', 'replacement_for',
                   'is_replacement', 'loss_processed' , 
                    'parent_lot', 'adjusted_sell_date', 'adjusted_proceeds' ]
        """
        ouput = []
        for lot in self._lots:
            row = {}
            if  convert_from_float(lot.num_shares) != 0.00 :

                row['num_shares'] = convert_from_float(lot.num_shares)
                row['symbol'] = lot.symbol
                row['description'] = lot.description
                row['buy_date'] = convert_from_date(lot.buy_date)
                
                if lot.buy_date == lot.adjusted_buy_date:
                    row['adjusted_buy_date'] = ''
                else:
                    row['adjusted_buy_date'] = convert_from_date(
                        lot.adjusted_buy_date)
                
                row['basis'] = convert_from_float(lot.basis)
                
                if lot.basis == lot.adjusted_basis:
                    row['adjusted_basis'] = 0
                else:
                    row['adjusted_basis'] = convert_from_float(lot.adjusted_basis)
                
                row['sell_date'] = convert_from_date(lot.sell_date)
                row['proceeds'] = convert_from_float(lot.proceeds)
                row['adjustment_code'] = lot.adjustment_code
                row['adjustment'] = convert_from_float(lot.adjustment)
                row['form_position'] = lot.form_position
                row['buy_lot'] = lot.buy_lot
                row['replacement_for'] = lot.replacement_for
                row['is_replacement'] = convert_from_bool(lot.is_replacement)
                row['loss_processed'] = convert_from_bool(lot.loss_processed)
                row['parent_lot'] = lot.parent_lot

                if lot.sell_date == lot.adjusted_sell_date:
                    row['adjusted_sell_date'] = ''
                else:
                    row['adjusted_sell_date'] = convert_from_date(
                        lot.adjusted_sell_date)   
               
                
                if lot.proceeds == lot.adjusted_proceeds:
                    row['adjusted_proceeds'] = ''
                else:
                    row['adjusted_proceeds'] = convert_from_float(lot.adjusted_proceeds)
                
                ouput.append(row)
            
        return ouput
        
# picked_transaction = dill.dumps(Transaction)
# picked_transactions = dill.dumps(Transactions)

# ofile = open("Transaction", "wb")
# dill.dump(Transaction, ofile)
# ofile.close()

# ofile = open("Transactions", "wb")
# dill.dump(Transactions, ofile)
# ofile.close()