import copy
import datetime
import Transaction as lots_lib
from operator import itemgetter
import operator

import pickle


def _split_lot_R(num_shares, lot, lots,  type_of_lot,
               existing_loss_lot=None, existing_replacement_lot=None, otherLot = None):
        """Splits lot and adds the new lot to lots.

        Args:
        num_shares: float, the number of shares that lot should contain.
            The split out lot will contain lot.num_shares - num_shares.
        lot: A Lot object to split.
        lots: A Lots object to add the new lot to.

        """

        existing_lot_portion = float(abs(num_shares)) / float(abs(lot.num_shares))

        new_lot_portion = float(1 - existing_lot_portion)

        new_lot = copy.deepcopy(lot)
        lots.add(new_lot)

        # if lot.num_shares < 0 :
        #     lots = sorted(lots, key=itemgetter(7)) 
        #     #lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_sell_key, lots_lib.Transactions.get_lot_key) )
        # else :
        #     lots = sorted(lots, key=itemgetter(3)) 
            #lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_buy_key, lots_lib.Transactions.get_lot_key) )
        
        new_lot.num_shares += num_shares
        #new_lot.num_shares = float(round(new_lot.num_shares * new_lot_portion))

        if new_lot.buy_date != '' and new_lot.sell_date =='' :

            new_lot.basis = float(new_lot.basis * new_lot_portion)
            #new_lot.adjusted_basis = float(new_lot.adjusted_basis * new_lot_portion)
            
        if new_lot.sell_date != '' and new_lot.buy_date =='' :

            new_lot.proceeds = float(new_lot.proceeds * new_lot_portion)
            #new_lot.adjusted_proceeds = float(new_lot.adjusted_proceeds * new_lot_portion)
            
        lot.num_shares = -num_shares
        #lot.num_shares = float(round(lot.num_shares * existing_lot_portion))

        if lot.buy_date != '' and lot.sell_date == '' :
            lot.basis = float(lot.basis * existing_lot_portion)
            #lot.adjusted_basis = float(lot.adjusted_basis * existing_lot_portion)
            otherLot.basis = lot.basis
            otherLot.buy_date = lot.buy_date
            otherLot.parent_lot =  lot.buy_lot
            
            lot.num_shares =0.00 ## buy is fully consumed
            lot.basis = 0.00

            lots.remove(lot)


        if lot.sell_date != '' and lot.buy_date == '' :
            lot.proceeds = float(lot.proceeds * existing_lot_portion)
            #lot.adjusted_proceeds = float(lot.adjusted_proceeds * existing_lot_portion)
            lot.buy_date = otherLot.buy_date
            lot.basis = float(otherLot.basis )
            
            lot.parent_lot =  otherLot.buy_lot
            
            otherLot.num_shares =0.00 ## buy is fully consumed
            otherLot.basis = 0.00
            lots.remove(otherLot)

print(pickle.loads(pickle.dumps(_split_lot_R, protocol= 4)))

def earliest_buy_lot(loss_lot, lots: lots_lib.Transactions):
    """Finds the best buy lot for a sell lot.

    The search starts from the earliest buy, and continues forward in time.

    Args:
        sell_lot: A Lot object.
        lots: A Lots object, the full set of lots.
    Returns:
        A Lot object, the best replacement lot, or None if there is none. May
        have more or fewer shares than the loss_lot.
    """
    
    def buy_list(e):
        return (e.num_shares > 0 and loss_lot.symbol == e.symbol and 
                e.sell_date == '' and e.buy_date != '' 
                and not e.is_replacement and not e.loss_processed
                and not e.buy_lot == loss_lot.parent_lot
                and not e.buy_lot == loss_lot.replacement_for)

    buy_lots = list(filter(buy_list, lots))
     
    buy_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key, lots_lib.Transactions.get_buy_key , lots_lib.Transactions.get_lot_key) )

    
    if not buy_lots:
        return None

    
    return buy_lots[0]


def split_one_lot_for_Wash(loss_lot, lots):
    
    replacement_lot = best_replacement_lot(loss_lot, lots)

    if not replacement_lot:
        #logger.print_lots('No replacement lot', lots, loss_lots=[loss_lot])
        loss_lot.loss_processed = True
        return


    if abs(loss_lot.num_shares) > abs(replacement_lot.num_shares):
        _split_lot(replacement_lot.num_shares, loss_lot, lots,  'loss',
                   existing_replacement_lot=replacement_lot,loss_lot=loss_lot)


    elif abs(replacement_lot.num_shares) > abs(loss_lot.num_shares):
        _split_lot(loss_lot.num_shares, replacement_lot, lots,
                        'replacement', existing_loss_lot=loss_lot,loss_lot=loss_lot)
                    
       
    if not IsShortSale(loss_lot) :

        loss_lot.loss_processed = True
        replacement_lot.is_replacement = True
        replacement_lot.replacement_for = loss_lot.buy_lot
        
    else :

        loss_lot.loss_processed = True
        replacement_lot.is_replacement = True
        replacement_lot.replacement_for = loss_lot.buy_lot
                
        

def wash_one_lot(loss_lot, lots):
    """Performs a single wash.

    Given a single loss lot, finds replacement lot(s) and adjusts their basis
    and buy date in place.

    If the loss lot needs to be split into multiple parts (because the
    replacement lots are for fewer shares) then it will be split into two parts
    and the wash will be performed for only the first part. The second part can
    be taken care of by another call to this method with it passed in as the
    loss_lot.

    If the replacement lot needs to be split into multiple parts (because the
    replacement lot has more shares than the loss lot) then it will be split
    and the second part of the lot will be added to lots.

    A replacement lot is one that is purchased within 30 days of the loss_lot's
    sale, not already used as a replacement, and not part of the same lot as
    the loss_lot.

    Args:
        loss_lot: A Lot object, which is a loss that should be washed.
        lots: A Lots object, the full set of lots.
        logger: A logger_lib.Logger.
    """
    replacement_lot = best_replacement_lot(loss_lot, lots)

    if not replacement_lot:
        #logger.print_lots('No replacement lot', lots, loss_lots=[loss_lot])
        loss_lot.loss_processed = True
        return



    if abs(loss_lot.num_shares) > abs(replacement_lot.num_shares):
        _split_lot(replacement_lot.num_shares, loss_lot, lots,  'loss',
                   existing_replacement_lot=replacement_lot,loss_lot=loss_lot)


    elif abs(replacement_lot.num_shares) > abs(loss_lot.num_shares):
        _split_lot(loss_lot.num_shares, replacement_lot, lots,
                   'replacement', existing_loss_lot=loss_lot,loss_lot=loss_lot)



    if not IsShortSale(loss_lot) :

        loss_lot.loss_processed = True
        loss_lot.adjustment_code = 'W'
        loss_lot.adjustment = abs(float(loss_lot.basis) + float(loss_lot.proceeds))
        replacement_lot.is_replacement = True
        replacement_lot.replacement_for = loss_lot.buy_lot
        replacement_lot.adjusted_basis  =  float(replacement_lot.basis) - float(loss_lot.adjustment )
        replacement_lot.adjusted_buy_date = loss_lot.buy_date ## change due IRS 2009
    else :

        loss_lot.loss_processed = True
        loss_lot.adjustment_code = 'W'
        loss_lot.adjustment = abs(float(loss_lot.basis) + float(loss_lot.proceeds))
        replacement_lot.is_replacement = True
        replacement_lot.replacement_for = loss_lot.buy_lot
        
        replacement_lot.adjusted_basis =  float(replacement_lot.proceeds) -  float(loss_lot.adjustment) 
        replacement_lot.adjusted_sell_date = loss_lot.sell_date ## change due IRS 2009


def _split_lot(num_shares, lot, lots,  type_of_lot,
               existing_loss_lot=None, existing_replacement_lot=None, loss_lot = None):
        """Splits lot and adds the new lot to lots.



        """
    
        if not IsShortSale(loss_lot) :

            existing_lot_portion = float(abs(num_shares)) / float(abs(lot.num_shares))

            new_lot_portion = float(1 - existing_lot_portion)

            new_lot = copy.deepcopy(lot)
            lots.add(new_lot)
            #lots = sorted(lots, key=itemgetter(1,3,7))

            new_lot.num_shares += num_shares

            new_lot.basis = float(new_lot.basis * new_lot_portion)
            
            new_lot.proceeds = float(new_lot.proceeds * new_lot_portion)
            new_lot.adjustment = float(new_lot.adjustment * new_lot_portion)

            lot.num_shares = -num_shares
            lot.basis = float(lot.basis * existing_lot_portion)
            lot.proceeds = float(lot.proceeds * existing_lot_portion)
            lot.adjustment = float(lot.adjustment * existing_lot_portion)

        else :

            existing_lot_portion = float(abs(num_shares)) / float(abs(lot.num_shares))

            new_lot_portion = float(1 - existing_lot_portion)

            new_lot = copy.deepcopy(lot)
            lots.add(new_lot)
            #lots = sorted(lots, key=itemgetter(1,3,7))

            new_lot.num_shares = float(round(new_lot.num_shares * new_lot_portion)) 

            new_lot.basis = float(new_lot.basis * new_lot_portion)
            
            new_lot.proceeds = float(new_lot.proceeds * new_lot_portion)
            new_lot.adjustment = float(new_lot.adjustment * new_lot_portion)

            lot.num_shares = float(round(lot.num_shares * existing_lot_portion)) 
            lot.basis = float(lot.basis * existing_lot_portion)
            lot.proceeds = float(lot.proceeds * existing_lot_portion)
            lot.adjustment = float(lot.adjustment * existing_lot_portion)


def IsShortSale(lot) :

    if lot.buy_date != '' and lot.sell_date != '' :
        
        if lot.buy_date > lot.sell_date :
           return True     
        elif lot.buy_date == lot.sell_date  and lot.buy_lot < lot.parent_lot :
            return True
        
        elif lot.buy_date < lot.sell_date :
            return False
        elif lot.buy_date == lot.sell_date and lot.buy_lot > lot.parent_lot :
            return False
    

def best_replacement_lot(loss_lot, lots):
    """Finds the best replacement lot for a loss lot.

    The search starts from the earliest buy, and continues forward in time. A
    replacement lot must be within 30 days on either side of the loss sale, not
    be part of the same lot, and not already have been used as a replacement.
    If there is only one lot bought on the first such day, then that is
    returned. It may be for fewer, the same, or more shares than the loss lot.
    If there are multiple lots bought on the first such day, then the one sold
    earliest is chosen. If there are multiple lots bought and sold on the same
    day, then the first lot by form position is chosen. For this reason, it is
    best to set a unique form position for each input line.

    If a potential replacement lot is sold before the loss lot is sold, that
    potential replacement lot is not considered. The reason for this is that it
    can push a loss arbitrarily far in the past, which means that it would be
    possible that subsequent year's tax returns would need to be amended. This
    seems wrong, so we don't allow for it. But there doesn't seem to be any IRS
    ruling on this issue, so it's up in the air whether this would present a
    problem. But IANACPA/IANAL.

    Args:
        loss_lot: A Lot object, which is a loss that should be washed.
        lots: A Lots object, the full set of lots.
    Returns:
        A Lot object, the best replacement lot, or None if there is none. May
        have more or fewer shares than the loss_lot.
    """
  
    def replacement_list(e):
        if not IsShortSale(loss_lot):
           return  (loss_lot.symbol == e.symbol 
                    and e.num_shares > 0 
                    and e.sell_date == '' and e.buy_date != ''
                    and  abs(loss_lot.sell_date - e.buy_date) <= datetime.timedelta(days=30)
                    and not e.is_replacement
                    and not e.buy_lot == loss_lot.parent_lot
                    and not e.buy_lot == loss_lot.buy_lot
                    and not e.buy_lot == loss_lot.replacement_for
                    and not e.loss_processed)
        else :
           return (loss_lot.symbol == e.symbol 
                   and e.num_shares < 0 
                   and e.sell_date != '' and e.buy_date == ''
                   and abs(loss_lot.buy_date - e.sell_date) <= datetime.timedelta(days=30)
                   and not e.is_replacement
                   and not e.buy_lot == loss_lot.replacement_for
                   and not e.buy_lot == loss_lot.buy_lot
                   and not e.buy_lot == loss_lot.parent_lot
                   and not e.loss_processed
                   )    
                #    and not not e.loss_processed )
    
    rep_lots = list(filter(replacement_list, lots))
    if not IsShortSale(loss_lot) :
        
        rep_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key , lots_lib.Transactions.get_buy_key,lots_lib.Transactions.get_lot_key) )
        ##rep_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key , lots_lib.Transactions.get_buy_key, lots_lib.Transactions.get_lot_key) )
    else :
        
        rep_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key,lots_lib.Transactions.get_sell_key, lots_lib.Transactions.get_lot_key) )    
        #rep_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key,lots_lib.Transactions.get_sell_key, lots_lib.Transactions.get_lot_key) )    

    
    if not rep_lots:
        return None
    
    return rep_lots[0]


def earliest_sell_lot(lots : lots_lib.Transactions):

    """Finds the first sell sale that has not already been processed.

    Args:
        lots: A Lots object, the full set of lots to search through.
    Returns:
        A Lot, or None.
    """
    
    def sell_list(e):
        return e.sell_date != ''  and e.buy_date == '' and e.num_shares < 0 and not e.loss_processed

    
    #lots_copy_s = copy.copy(lots)
    sell_lots = list(filter(sell_list, lots))
    
    sell_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key,lots_lib.Transactions.get_sell_key, lots_lib.Transactions.get_lot_key) )
    #sell_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key,lots_lib.Transactions.get_buy_key, lots_lib.Transactions.get_sell_key) )
    

    if not sell_lots:
        return None
    
    return sell_lots[0]


def rlzdunrlzd_one_lot(loss_lot, lots : lots_lib.Transactions):
    """Performs a single realization.

    Args:
        sell_lot: A Lot object
        lots: A Lots object, the full set of lots.
        logger: A logger_lib.Logger.
    """
    replacement_lot = earliest_buy_lot(loss_lot, lots)

    if not replacement_lot:
        

        loss_lot.loss_processed = True
        return


    if abs(loss_lot.num_shares) > abs(replacement_lot.num_shares) and abs(replacement_lot.num_shares) > 0:
        _split_lot_R(replacement_lot.num_shares, loss_lot, lots,  'sell',
                   existing_replacement_lot=replacement_lot, otherLot=replacement_lot)



    elif abs(replacement_lot.num_shares) > abs(loss_lot.num_shares) and abs(loss_lot.num_shares) > 0 :
        _split_lot_R(loss_lot.num_shares, replacement_lot, lots,
                   'buy', existing_loss_lot=loss_lot,otherLot=loss_lot)



    elif abs(replacement_lot.num_shares) == abs(loss_lot.num_shares) and abs(loss_lot.num_shares) > 0  :
        loss_lot.buy_date = replacement_lot.buy_date
        loss_lot.basis = replacement_lot.basis

        #loss_lot.adjusted_basis = replacement_lot.adjusted_basis
        #loss_lot.adjusted_buy_date = replacement_lot.adjusted_buy_date
        loss_lot.parent_lot =  replacement_lot.buy_lot

        replacement_lot.num_shares = 0.00
        replacement_lot.basis = 0.00
        #lots.remove(replacement_lot)

        
        
        
    
    loss_lot.loss_processed = False
    #loss_lot.adjustment_code = 'S' # -- relealized gain/loss processed
    replacement_lot.is_replacement = True


def loss_lots(lots : lots_lib.Transactions)  :

    def loss_list(e):
        return ( (e.proceeds + e.basis) < 0 
                  and e.buy_date != '' and e.sell_date != ''  
                )

    loss_lots = list(filter(loss_list, lots))

    return loss_lots


    

def earliest_loss_lot(lots : lots_lib.Transactions):

    """Finds the first loss sale that has not already been processed.

    Args:
        lots: A Lots object, the full set of lots to search through.
    Returns:
        A Lot, or None.
    """

    lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key , lots_lib.Transactions.get_lot_key ))    

    #lots_copy_l = copy.copy(lots)
    def loss_list(e):
        return ( (e.proceeds + e.basis) < 0 and not e.loss_processed
                  and e.buy_date != '' and e.sell_date != ''  
                )

    loss_lots = list(filter(loss_list, lots))

    #loss_lots = [e for e in lots if  (e.proceeds + e.basis) < 0 and e.is_loss() and not e.loss_processed]
    loss_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key,lots_lib.Transactions.get_buy_key,lots_lib.Transactions.get_sell_key )) #, lots_lib.Transactions.get_lot_key) )
    #loss_lots.sort(key= operator.attrgetter(lots_lib.Transactions.get_symbol_key,lots_lib.Transactions.get_sell_key,lots_lib.Transactions.get_lot_key) )
    
    if not loss_lots:
        return None

    
    return loss_lots[0]

def rlzdunrlzd_all_lots(lots :lots_lib.Transactions):
    """Performs realization of all the lots.

    Args:
        lots: A Lots object.
     """
    
    
    while True:

        sell_lot = earliest_sell_lot(lots)
        if not sell_lot:
              break
        
        rlzdunrlzd_one_lot(sell_lot, lots)


def Split_lots_to_Wash(lots :lots_lib.Transactions):
    """Performs wash sales of all the lots.

    
    """
    
    while True:

        loss_lot = earliest_loss_lot(lots)
        if not loss_lot:
            break
        split_one_lot_for_Wash(loss_lot, lots)
      


def wash_all_lots(lots :lots_lib.Transactions):
    """Performs wash sales of all the lots.

    
    """
    # lots_copy =  copy.deepcopy(lots)
    # lots_loss = loss_lots(lots_copy)

    while True:

        loss_lot = earliest_loss_lot(lots)
        if not loss_lot:
            break
        wash_one_lot(loss_lot, lots)

def returnOutput(object) -> list :
    lots =  lots_lib.Transactions.write_csv_data(object)
    return lots

def HandleinputFile(fileName : str) :
    lots = lots_lib.Transactions([])
    with open(fileName) as f:
      lots = lots_lib.Transactions.create_from_csv_data(f)

    rlzdunrlzd_all_lots(lots)
    l= returnOutput(lots)
    
    return l

def HandleListObject(ListObj : list, isrlzd : int) :
    lots = lots_lib.Transactions([])
    lots = lots_lib.Transactions.create_from_List_Object (ListObj)

    if isrlzd == 1:
         rlzdunrlzd_all_lots(lots)
         
         l= returnOutput(lots)
    
    # if isrlzd == 0:    
    #     pass
        #Split_lots_to_Wash(lots)
        #wash_all_lots(lots) 

        #l= returnOutput(lots)

    if isrlzd == 2:    
        #Split_lots_to_Wash(lots)
        wash_all_lots(lots) 

        l= returnOutput(lots)


    return l

#print(cloudpickle.loads(cloudpickle.dumps(HandleListObject)))

def HandleListObjectPerLot(ListObj : list) :
    lots = lots_lib.Transactions([])
    lots = lots_lib.Transactions.create_from_List_Object (ListObj)

    while True:

        sell_lot = earliest_sell_lot(lots)
        if not sell_lot:
             break

        rlzdunrlzd_one_lot(sell_lot, lots)
        
        loss_lot = earliest_loss_lot(lots)
        if not loss_lot:
            break
        wash_one_lot(loss_lot, lots)
        
    l= returnOutput(lots)

    return l

def updatelotsattribute(lots: lots_lib.Transactions,isafterrlzd : int) :
    
    if isafterrlzd == 1 :
        for lot in lots :
            lot.is_replacement = 'False'
            lot.replacement_for =''
            lot.loss_processed = 'False'
    else :
        
        for lot in lots :
             lot.is_replacement = 'False'
            

def HandleListObjectAllataTime(ListObj : list, isrlzd : int) :

    if isrlzd == 1 :

        lots = lots_lib.Transactions([])
        lots = lots_lib.Transactions.create_from_List_Object (ListObj)
        rlzdunrlzd_all_lots(lots)
        updatelotsattribute(lots,0)
        wash_all_lots(lots)
        l= returnOutput(lots)
        
    return l
    

