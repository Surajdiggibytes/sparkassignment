def user_data(userDf,transactionDf):
    # Count of unique locations where each product is sold
    left_join = transactionDf.join(userDf, userDf.user_id == transactionDf.user_id, how="left")
    final_df = left_join.select(userDf['location ']).distinct()
    fin = final_df.count()
    print(fin)
    return fin

    # # products bought by each user.
def products(transactionDf):
    second = transactionDf.select('user_id','product_description').orderBy('user_id')
    return second

    # # Total spending done by each user on each product
def spending(transactionDf):
    third = transactionDf.select("user_id", "product_description", "price").orderBy(transactionDf['user_id'])
    return third



