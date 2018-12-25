{
    "id": "APP_Calc_Subscr",
    "params": {
        "@id": "int" ,
        "@batch": "int" ,
        "@F_Division": "int" ,
        "@D_Date0": "smalldatetime" ,
        "@D_Date1": "smalldatetime",
        "()":"EXEC PY.PP_Init_Params @id, @batch, @F_Division OUTPUT, @D_Date0 OUTPUT, @D_Date1 OUTPUT"
    },
    "SD_Divisions": {
        "name": "SD_Divisions",
        "cols": ["LINK", "C_Name"],
        "filter": "@.LINK = @F_Division",

        "SD_Conn_Points": {
        "sql":"PY.PF_SD_Conn_Points(@id, @batch)",
            "ref":{"left":"LINK", "right":"F_Division"},
            "cols": ["LINK", "N_PaymentDay","F_Conn_Types", "F_Conn_Status_Types"],

            "SD_Conn_Points_Sub": {
            "sql":"PY.PF_SD_Conn_Points_Sub(@id, @batch)",
                "cols": ["C_Premise_Number", "F_Conn_Types", "F_Conn_Status_Types"],

                "SD_Contract_Squares": {
                    "cols": ["F_Prop_Forms", "D_Date", "D_Date_End", "N_Placement_Count", "N_Square"],
                    "filter": "@.D_Date<@D_Date1 AND @.D_Date_End>@D_Date0"
                },
                "SD_Subscr": {
            "sql":"PY.PF_SD_Subscr(@id, @batch, @F_Division)",
                    "cols": [
                        "N_Code", "F_Supplier", "D_Date_Begin", "D_Date_End"
                    ],
                    "filter": "@.D_Date_Begin<@D_Date1 AND (@.D_Date_End>@D_Date0 OR @.D_Date_End IS NULL) AND @.B_Receivable=1 AND @.B_EE=0 AND @.F_Conn_Points=@parent.F_Conn_Points",

                    "ED_Registr_Pts": {
                        "cols": [
                            { "F_Sale_Items": ["LINK", "C_Const", "F_Units", "N_Precision", "N_Precision2"] },
                "F_Energy_Levels", "F_Network_Pts", "F_Sale_Category", "D_Date_Begin", "D_Date_End"
                        ],
                        "filter": "@.D_Date_Begin<@D_Date1 AND (@.D_Date_End>@D_Date0 OR @.D_Date_End IS NULL)",

                        "ED_Registr_Pts_Calc_Methods": {
                            "cols": ["D_Date", "D_Date_End", "F_Calc_Methods", "F_Calc_Methods_Default"],
                            "filter": "@.D_Date<@D_Date1 AND @.D_Date_End>@D_Date0"
                        },

                        "ED_Registr_Pts_Tariff": {
                            "cols": [
                                "D_Date", "D_Date_End",
                                { "F_Tariff": ["LINK", "F_Energy_Levels", "F_Units", "F_Units_2", "F_Sale_Accounts_1", "F_Sale_Accounts_2", "F_Taxes"] }
                            ],
                            "filter": "@.D_Date<@D_Date1 AND @.D_Date_End>@D_Date0"
                        },

                        "ED_Registr_Pts_Cons": {
                            "cols": ["D_Date", "D_Date_End", "F_Time_Zones", "N_Cons", "N_Cons2"],
                            "filter": "@.D_Date<@D_Date1 AND @.D_Date_End>@D_Date0"
                        },

                        "ED_Registr_Pts_Activity":{
                            "cols":["N_Rate", "D_Date", "D_Date_End"],
                            "filter": "@.B_Double = 0 and @.D_Date<@D_Date1 AND @.D_Date_End>@D_Date0"
                            }
                    }
                }
            }
        }
    }
}
