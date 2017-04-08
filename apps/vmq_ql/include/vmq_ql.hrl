-record(vmq_ql_table, {
          name                  :: atom(),
          depends_on = []       :: [#vmq_ql_table{}],
          provides = []         :: [atom()],
          init_fun              :: function(),
          include_if_all = true :: boolean()
         }).
-type info_table()      :: #vmq_ql_table{}.
