def user_data(userDf,transactionDf):
    # Count of unique locations where each product is sold
    left_join = transactionDf.join(userDf, userDf.user_id == transactionDf.user_id, how="left")
    final_df = left_join.select(userDf['location ']).distinct()
    fin = final_df.count()
    # print(fin)
    return fin

    # products bought by each user.
    second = left_join.select(userDf['user_id'], transactionDf['product_description']).orderBy(transactionDf["user_id"])
    # second.show()
    return second

    # Total spending done by each user on each product
    third = transactionDf.select("user_id", "product_description", "price").orderBy(transactionDf['user_id'])
    # third.show()
    return third



