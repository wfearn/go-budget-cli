Planning on turning this into a budget cli that I can use for my own purposes.

TODOS:
1. Investigate transaction retrieval APIs
2. Investigate secure storage solutions
3. Write up design doc
4. Write unit tests
5. Implement CLI


# Interface

## Main Commands

### budget (name tbd)

    $budget summary --month
    Summary for current month: Month_X
        Total Income So Far: $9999 (Y% of $9999 Expected)
        Total Expenses So Far: $9999 (L% of $9999 Expected)
            Budget 1: $9999 of $99999 used (Q%) ==> $9999 (U%) below Monthly Average
            Budget 2: $999 of $99999 used (Z%) ==> $9999 (A%) above Monthly Average
            ...
            Budget N: $9999 of $99 used (P%) ==> $9999 (F%) below Monthly Average
