/** Copyright © 2017-2020, GSPANN Technologies and/or its affiliates. All rights reserved. * */
package beat;

import net.sf.jsqlparser.JSQLParserException;
import org.hibernate.engine.jdbc.internal.BasicFormatterImpl;
import org.hibernate.engine.jdbc.internal.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Adithya
 */
public class QueryGenerator {

    private final static Logger LOGGER = LoggerFactory.getLogger(QueryGenerator.class);

    private SqlParser sqlParser;
    private String qry;
    private Formatter formatter;

    public QueryGenerator() {
        LOGGER.info("Querygenerator Object Created ");
        sqlParser = new SqlParser();
        formatter = new BasicFormatterImpl();
    }

    public String getTotalCntQueries(String schema, String tblname, String data, String qrGen, String hostType, String trgRule, String dbType, String incRule) {
        if (qrGen.equalsIgnoreCase("db")) {

            if (hostType.equalsIgnoreCase("trg")) {

                if (!trgRule.isEmpty()) {
                    if (!trgRule.toLowerCase().contains("where")) {
                        qry = "select count(1) from " + schema + "." + tblname + " where " + trgRule;
                    } else {
                        qry = "select count(1) from " + schema + "." + tblname + " " + trgRule;
                    }

                } else {
                    qry = "select count(1) from " + schema + "." + tblname;
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }
            } else {
                if (!incRule.isEmpty()) {
                    if (!data.toLowerCase().contains("where")) {
                        data = data + " where " + incRule;
                    } else {
                        String tmpQry = data.substring(0, data.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + data.substring(data.lastIndexOf("where") + 5, data.length());
                        data = tmpQry;
                    }
                }

                qry = "select count(1) from (" + data + " ) " + tblname;

            }
        } else {
            LOGGER.info("tableName: " + tblname);
            if (!trgRule.isEmpty()) {
                if (!trgRule.toLowerCase().contains("where")) {
                    qry = "select count(1) from \"" + tblname + "\" where " + trgRule;
                } else {
                    qry = "select count(1) from \"" + tblname + "\" " + trgRule;
                }
            } else {
                qry = "select count(1) from \"" + tblname + "\" ";
            }

            LOGGER.info("tableNameGot: " + qry);
            if (!incRule.isEmpty()) {
                if (!qry.toLowerCase().contains("where")) {
                    qry = qry + " where " + incRule;
                } else {
                    String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                    qry = tmpQry;
                }

            }
        }
        if (dbType.equalsIgnoreCase("db2")) {
            qry += qry + " with ur";
        }
        LOGGER.info("Total Count Query Generated ");

        return formatter.format(qry);
    }

    public String getNullCntQueries(String schema, String tblname, String data, String type, String hostType, String trgRule, String dbType, String incRule, String conn) throws JSQLParserException, Exception {
        if (type.equalsIgnoreCase("db")) {
            if (hostType.equalsIgnoreCase("trg")) {
                System.out.println("Trg Rule: " + trgRule + " : " + trgRule.isEmpty());
                if (!trgRule.isEmpty()) {
                    if (!trgRule.toLowerCase().contains("where")) {
                        System.out.println("Trg Rule: " + trgRule + " : " + trgRule.isEmpty());
                        qry = "select count(1) as " + data.trim() + " from " + schema + "." + tblname + " where " + trgRule + " and " + data.trim() + " is null";// or "+data.trim()+" = '') and " + trgRule;
                    } else {
                        qry = "select count(1) as " + data.trim() + " from " + schema + "." + tblname + " " + trgRule + " and  " + data.trim() + " is null ";//or "+data.trim()+" = '' )";
                    }
                } else {
                    qry = "select count(1) as " + data.trim() + " from " + schema + "." + tblname + " where " + data.trim() + " is null ";//or "+data.trim()+" = ''";
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }
            } //Check the Data
            else {
                if (!incRule.isEmpty()) {
                    if (!data.toLowerCase().contains("where")) {
                        data = data + " where " + incRule;
                    } else {
                        String tmpQry = data.substring(0, data.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + data.substring(data.lastIndexOf("where") + 5, data.length());
                        data = tmpQry;
                    }
                }
                qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(data) + " from ( " + data + ") " + tblname + " where " + tblname + "." + sqlParser.getColumnNamefromQuery(data) + " is null ";//or "+sqlParser.getColumnNamefromQuery(data)+" = ''";

            }
        } else {

            if (hostType.equalsIgnoreCase("trg")) {
                if (trgRule.isEmpty()) {
                    qry = "select count(1) as " + data.trim() + " from \"" + tblname + "\" where lower(" +  data.trim() + ") is null ";// or lower(" + data.trim() + ") ='' ";
                } else {
                    if (trgRule.toLowerCase().startsWith("where")) {
                        qry = "select count(1) as " + data.trim() + " from \"" + tblname + "\" " + trgRule + " and (lower(" + data.trim() + ") = 'null";// or lower(" + data.trim() + ") ='')";
                    } else {
                        qry = "select count(1) as " + data.trim() + " from \"" + tblname + "\" where (lower(" + data.trim() + ") is  null";// or lower(" + data.trim() + ") ='')   and " + trgRule;
                    }
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }
            } else {

                String tmp = data.trim();

                if (trgRule.isEmpty()) {
                    qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\" where " + sqlParser.getColumnNamefromQuery(tmp) + " is  null";// or " + sqlParser.getColumnNamefromQuery(tmp) + " = '' ";
                } else {
                    if (trgRule.toLowerCase().startsWith("where")) {
                        qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\"  " + trgRule + " and (" + sqlParser.getColumnNamefromQuery(tmp) + " is  null";//  or " + sqlParser.getColumnNamefromQuery(tmp) + " = '')";
                    } else {
                        qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\"  where (" + sqlParser.getColumnNamefromQuery(tmp) + " is  null";// or " + sqlParser.getColumnNamefromQuery(tmp) + " = '' ) and " + trgRule;

                    }
                }

            }
        }
        LOGGER.info("Total Null Count Query Generated ");

        return qry;

    }

    public String getNotNullCntQueries(String schema, String tblname, String data, String type, String hostType, String trgRule, String incRule, String conn) throws JSQLParserException, Exception {

        if (type.equalsIgnoreCase("db")) {
            if (hostType.equalsIgnoreCase("trg")) {
                if (!trgRule.isEmpty()) {
                    if (!trgRule.toLowerCase().contains("where")) {
                        qry = "select count(1) as " + data.trim() + " from " + schema + "." + tblname + " where " + trgRule + " and " + data.trim() + " is not null";// or lower(" + data.trim() + ") !='') and " + trgRule;
                    } else {
                        qry = "select count(1) as " + data.trim() + " from " + schema + "." + tblname + " " + trgRule + " and ( " + data.trim() + " is not null)";//  or lower(" + data.trim() + ") !='')";
                    }
                } else {
                    qry = "select count(1) as " + data.trim() + " from " + schema + "." + tblname + " where " + data.trim() + " is not null";//  or lower(" + data.trim() + ") !=''";
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }
            } else {
                if (!incRule.isEmpty()) {
                    if (!data.toLowerCase().contains("where")) {
                        data = data + " where " + incRule;
                    } else {
                        String tmpQry = data.substring(0, data.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + data.substring(data.lastIndexOf("where") + 5, data.length());
                        data = tmpQry;
                    }
                }
                qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(data) + " from (" + data + ")" + tblname + " where " + tblname + "." + sqlParser.getColumnNamefromQuery(data) + " is  not null";//  or lower(" + sqlParser.getColumnNamefromQuery(data) + ") !='' ";
            }
        } else {
            if (hostType.equalsIgnoreCase("trg")) {
//                qry = "select count(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" where lower(" + data.trim() + ") != null and lower(" + data.trim() + ") !=''  ";

                if (trgRule.isEmpty()) {
                    qry = "select count(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" where lower(" + data.trim() + ") is not null";// and lower(" + data.trim() + ") !='' ";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {
                        qry = "select count(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" " + trgRule + " and ( lower(" + data.trim() + ") is not null)";// and lower(" + data.trim() + ") !='' )";
                    } else {
                        qry = "select count(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" where (lower(" + data.trim() + ") is not null)";// and lower(" + data.trim() + ") !='' ) and " + trgRule;
                    }
                }

            } else {

                String tmp = data.trim();

                if (trgRule.isEmpty()) {
                    qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\" where lower(" + sqlParser.getColumnNamefromQuery(tmp) + ") is not null";// and lower(" + sqlParser.getColumnNamefromQuery(tmp) + ") != ''";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

                        qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\" " + trgRule + " and lower(" + sqlParser.getColumnNamefromQuery(tmp) + ") is not null";// and lower(" + sqlParser.getColumnNamefromQuery(tmp) + ") != '')";
                    } else {
                        qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\" where (lower(" + sqlParser.getColumnNamefromQuery(tmp) + ") is not null";// and lower(" + sqlParser.getColumnNamefromQuery(tmp) + ") != '') and " + trgRule;
                    }
                }
            }

        }

        LOGGER.info("Total Null not Count Query Generated ");

        return qry;

    }

    public String getDistinctCntQueries(String schema, String tblname, String data, String type, String hostType, String trgRule, String incRule, String conn) throws JSQLParserException, Exception {

        if (type.equalsIgnoreCase("db")) {
            if (hostType.equalsIgnoreCase("src")) {
                if (!incRule.isEmpty()) {
                    if (!data.toLowerCase().contains("where")) {
                        data = data + " where " + incRule;
                    } else {
                        String tmpQry = data.substring(0, data.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + data.substring(data.lastIndexOf("where") + 5, data.length());
                        data = tmpQry;
                    }
                }
                qry = "select count(distinct(" + tblname + "." + sqlParser.getColumnNamefromQuery(data) + ")) as " + sqlParser.getColumnNamefromQuery(data) + "  from (" + data + ")" + tblname + " where " + tblname + "." + sqlParser.getColumnNamefromQuery(data) + " is not null";
            } else {
                if (!trgRule.isEmpty()) {
                    if (!trgRule.toLowerCase().contains("where")) {
                        qry = "select count(distinct(" + tblname + "." + data.trim() + ")) as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + " where (" + trgRule + " ) and  " + data.trim() + " is not null";;
                    } else {
                        qry = "select count(distinct(" + tblname + "." + data.trim() + ")) as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + "  " + trgRule + " and " + data.trim() + " is not null";;
                    }
                } else {

                    qry = "select count(distinct(" + tblname + "." + data.trim() + ")) as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + "  where " + data.trim() + " is not null";;
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }
            }
        } else {

            System.out.println("Trg Rule: " + trgRule);
            if (hostType.equalsIgnoreCase("trg")) {
//                qry = "select count(distinct(" + data.trim() + ")) as " + data.trim() + " from \"" + tblname + "\" where " + data.trim() + " !=null";

                if (trgRule.isEmpty()) {
//                    qry = "select count(distinct(" + data.trim() + ")) as " + data.trim() + " from \"" + tblname + "\" where " + data.trim() + " !=null";
                    qry = "select count(1) as " + data.trim() + "  from (select distinct(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" where lower(" + data.trim() + ") is not null) a";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

                        qry = "select count(1) as " + data.trim() + "  from (select distinct(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" " + trgRule + " and lower(" + data.trim() + ") is not null) a";
//                        qry = "select count(distinct(" + data.trim() + ")) as " + data.trim() + " from \"" + tblname + "\" " + trgRule + " and " + data.trim() + " !=null";
                    } else {
//                        qry = "select count(distinct(" + data.trim() + ")) as " + data.trim() + " from \"" + tblname + "\" where " + trgRule + " and " + data.trim() + " !=null";
                        qry = "select count(1) as " + data.trim() + "  from (select distinct(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" where " + trgRule + " and lower(" + data.trim() + ") is not null) a";

                    }
                }

            } else {

                String tmp = data.trim();

                if (trgRule.isEmpty()) {

//                    qry = "select count(distinct(" + sqlParser.getColumnRulefromQuery(tmp) + ")) as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\" where " + sqlParser.getColumnRulefromQuery(tmp) + " !=null";
                    qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(tmp) + "  from (select distinct(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\" where " + sqlParser.getColumnNamefromQuery(tmp) + " is not null) a";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

//                        qry = "select count(distinct(" + sqlParser.getColumnRulefromQuery(tmp) + ")) as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\" " + trgRule + " and " + sqlParser.getColumnRulefromQuery(tmp) + " !=null";
                        qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(tmp) + "  from (select distinct(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\" " + trgRule + " and " + sqlParser.getColumnNamefromQuery(tmp) + " !=null) a";
                    } else {
//                        qry = "select count(distinct(" + sqlParser.getColumnRulefromQuery(tmp) + ")) as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\" where " + trgRule + " and " + sqlParser.getColumnRulefromQuery(tmp) + " !=null";
                        qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(tmp) + "  from (select distinct(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from \"" + tblname + "\" where " + trgRule + " and " + sqlParser.getColumnNamefromQuery(tmp) + " !=null) a";
                    }
                }

            }
        }
        LOGGER.info("Total Distinct Count Query Generated ");

        return qry;

    }

    public String getDuplicateCntQueries(String schema, String tblname, String data, String type, String hostType, String trgRule, String dbType, String incRule, String conn) throws JSQLParserException, Exception {

        if (type.equalsIgnoreCase("db")) {
            if (hostType.equalsIgnoreCase("src")) {
                if (!incRule.isEmpty()) {
                    if (!data.toLowerCase().contains("where")) {
                        data = data + " where " + incRule;
                    } else {
                        String tmpQry = data.substring(0, data.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + data.substring(data.lastIndexOf("where") + 5, data.length());
                        data = tmpQry;
                    }
                }
                if (dbType.trim().equalsIgnoreCase("bigquery")) {
                    qry = "select count(" + sqlParser.getColumnNamefromQuery(data) + ") from (" + data + ")" + tblname + " where (" + sqlParser.getColumnNamefromQuery(data) + " is not null ) group by " + sqlParser.getColumnNamefromQuery(data) + " having count(" + sqlParser.getColumnNamefromQuery(data) + ") >1";

                } else {

                    qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(data) + " from (" + data + ")a where (a." + sqlParser.getColumnNamefromQuery(data) + " is not null ) group by a." + sqlParser.getColumnNamefromQuery(data) + " having count(a." + sqlParser.getColumnNamefromQuery(data) + ") >1";
                }
            } else {
                if (!trgRule.isEmpty()) {
                    if (!trgRule.toLowerCase().contains("where")) {
                        if (dbType.trim().equalsIgnoreCase("bigquery")) {
                            qry = "select count(" + data + ") from " + schema + "." + tblname + " where (" + data + " is not null ) and " + trgRule + " group by " + data + " having count(" + data + ") >1";
                        } else {
                            qry = "select count(1) as " + data + " from " + schema + "." + tblname + " " + tblname + "  where (" + tblname + "." + data + " is not null ) and " + trgRule + " group by " + tblname + "." + data + " having count(" + tblname + "." + data + ") >1";
                        }
                    } else {
                        if (dbType.trim().equalsIgnoreCase("bigquery")) {
                            qry = "select count(" + data + ")  from " + schema + "." + tblname + "  " + trgRule + " and (" + data + " is not null ) group by " + data + " having count(" + data + ") >1";
                        } else {
                            qry = "select count(1) as " + data + " from " + schema + "." + tblname + " " + tblname + "  " + trgRule + " and (" + tblname + "." + data + " is not null ) group by " + tblname + "." + data + " having count(" + tblname + "." + data + ") >1";
                        }
                    }
                } else {
                    if (dbType.trim().equalsIgnoreCase("bigquery")) {
                        qry = "select count(" + data + ")  from " + schema + "." + tblname + " where " + data + " is not null group by " + data + " having count(" + data + ") >1";
                    } else {
                        qry = "select count(1) as " + data + " from " + schema + "." + tblname + " " + tblname + " where " + tblname + "." + data + " is not null group by " + tblname + "." + data + " having count(" + tblname + "." + data + ") >1";
                    }
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }
            }
        } else {

            if (hostType.equalsIgnoreCase("trg")) {

                if (trgRule.isEmpty()) {
//                    qry = "select count(" + data.trim() + ") as " + data + " from \"" + tblname + "\" where " + data + "  != null group by " + data + " having count(" + data + ") >1";
                    qry = "select count(1) as " + data + "," + data.trim() + "  from \"" + tblname + "\" where " + data.trim() + "  is not null group by " + data + " having count(" + data + ") >1";

                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

//                        qry = "select count(" + data.trim() + ") as " + data + " from \"" + tblname + "\" " + trgRule + " and (" + data + "  != null )  group by " + data + " having count(" + data + ") >1";
                        qry = "select count(1) as " + data + "," + data.trim() + "  from \"" + tblname + "\" " + trgRule + " and (" + data.trim() + "  is not null )  group by " + data + " having count(" + data + ") >1) a";
                    } else {
//                        qry = "select count(" + data.trim() + ") as " + data + " from \"" + tblname + "\" where (" + data + "  != null ) and " + trgRule + " group by " + data + " having count(" + data + ") >1";

                        qry = "select count(1) as " + data + "," + data.trim() + "  from \"" + tblname + "\" where (" + data.trim() + "  is not null ) and " + trgRule + " group by " + data + " having count(" + data + ") >1) a";
                    }
                }

            } else {

                String tmp = data.trim();

                if (trgRule.isEmpty()) {
//                    qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + "  from  \"" + tblname + "\" where " + sqlParser.getColumnNamefromQuery(tmp) + "  != null  group by " + sqlParser.getColumnRulefromQuery(tmp) + " having count(" + sqlParser.getColumnRulefromQuery(tmp) + ") >1";

                    qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(tmp) + " , " + sqlParser.getColumnRulefromQuery(tmp) + "  from  \"" + tblname + "\" where " + sqlParser.getColumnNamefromQuery(tmp) + "  is not null  group by " + sqlParser.getColumnRulefromQuery(tmp) + " having count(" + sqlParser.getColumnRulefromQuery(tmp) + ") >1";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

//                        qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + "   from  \"" + tblname + "\" " + trgRule + " and (" + sqlParser.getColumnNamefromQuery(tmp) + "  != null ) group by " + sqlParser.getColumnRulefromQuery(tmp) + " having count(" + sqlParser.getColumnRulefromQuery(tmp) + ") >1";
                        qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(tmp) + ", " + sqlParser.getColumnRulefromQuery(tmp) + " as " + sqlParser.getColumnNamefromQuery(tmp) + "   from  \"" + tblname + "\" " + trgRule + " and (" + sqlParser.getColumnNamefromQuery(tmp) + "  != null ) group by " + sqlParser.getColumnRulefromQuery(tmp) + " having count(" + sqlParser.getColumnRulefromQuery(tmp) + ") >1 ";
                    } else {
//                        qry = "select count(" + sqlParser.getColumnRulefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + "  from  \"" + tblname + "\" where (" + sqlParser.getColumnNamefromQuery(tmp) + " != null ) and " + trgRule + " group by " + sqlParser.getColumnRulefromQuery(tmp) + " having count(" + sqlParser.getColumnRulefromQuery(tmp) + ") >1";
                        qry = "select count(1) as " + sqlParser.getColumnNamefromQuery(tmp) + " , " + sqlParser.getColumnNamefromQuery(tmp) + "  from  \"" + tblname + "\" where (" + sqlParser.getColumnNamefromQuery(tmp) + " != null ) and " + trgRule + " group by " + sqlParser.getColumnRulefromQuery(tmp) + " having count(" + sqlParser.getColumnRulefromQuery(tmp) + ") >1";
                    }

                }
            }

        }
        LOGGER.info("Total Duplicate Query Generated : " + qry);

        return qry;

    }

    public String getMaxColQueries(String schema, String tblname, String data, String type, String hostType, String trgRule, String incRule) throws JSQLParserException {

        if (type.equalsIgnoreCase("db")) {
            if (hostType.equalsIgnoreCase("src")) {
                if (!incRule.isEmpty()) {
                    if (!data.toLowerCase().contains("where")) {
                        data = data + " where " + incRule;
                    } else {
                        String tmpQry = data.substring(0, data.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + data.substring(data.lastIndexOf("where") + 5, data.length());
                        data = tmpQry;
                    }
                }
                qry = "select max(" + tblname + "." + sqlParser.getColumnNamefromQuery(data) + ") as " + sqlParser.getColumnNamefromQuery(data) + " from (" + data + ")" + tblname;
            } else {
                if (!trgRule.isEmpty()) {
                    if (!trgRule.toLowerCase().contains("where")) {
                        qry = "select max(" + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + " where " + trgRule;
                    } else {
                        qry = "select max(" + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + "  " + trgRule;
                    }
                } else {
                    qry = "select max(" + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname;
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }

            }
        } else {

            if (hostType.equalsIgnoreCase("trg")) {

                if (trgRule.isEmpty()) {
                    qry = "select max(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\"";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

                        qry = "select max(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" " + trgRule;
                    } else {
                        qry = "select max(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" where " + trgRule;
                    }
                }

            } else {

                String tmp = data.trim();

                if (trgRule.isEmpty()) {
                    qry = "select max(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\"";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

                        qry = "select max(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\" " + trgRule;
                    } else {
                        qry = "select max(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\" where " + trgRule;
                    }
                }
            }

        }
        LOGGER.info("Total Max Count Query Generated ");

        return qry;

    }

    public String getMinColQueries(String schema, String tblname, String data, String type, String hostType, String trgRule, String incRule) throws JSQLParserException {

        if (type.equalsIgnoreCase("db")) {
            if (hostType.equalsIgnoreCase("src")) {
                if (!incRule.isEmpty()) {
                    if (!data.toLowerCase().contains("where")) {
                        data = data + " where " + incRule;
                    } else {
                        String tmpQry = data.substring(0, data.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + data.substring(data.lastIndexOf("where") + 5, data.length());
                        data = tmpQry;
                    }
                }
                qry = "select min(" + tblname + "." + sqlParser.getColumnNamefromQuery(data) + ") as " + sqlParser.getColumnNamefromQuery(data) + " from (" + data + ")" + tblname;
            } else {
                if (!trgRule.isEmpty()) {
                    if (!trgRule.toLowerCase().contains("where")) {
                        qry = "select min(" + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + " where " + trgRule;
                    } else {
                        qry = "select min(" + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + " " + trgRule;
                    }
                } else {
                    qry = "select min( " + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname;
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }
            }
        } else {

            if (hostType.equalsIgnoreCase("trg")) {

                if (trgRule.isEmpty()) {
                    qry = "select min(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\"";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

                        qry = "select min(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" " + trgRule;
                    } else {
                        qry = "select min(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" where " + trgRule;
                    }
                }
            } else {

                String tmp = data.trim();

                if (trgRule.isEmpty()) {
                    qry = "select min(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\"";
                } else {

                    if (trgRule.toLowerCase().startsWith("where")) {

                        qry = "select min(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\" " + trgRule;
                    } else {
                        qry = "select min(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\" where " + trgRule;
                    }
                }
            }

        }
        LOGGER.info("Total Min Count Query Generated ");

        return qry;

    }

    public String getSumColQueries(String schema, String tblname, String data, String type, String hostType, String dtype, String trgRule, String incRule) throws JSQLParserException {

        if (type.equalsIgnoreCase("db")) {
            if (hostType.equalsIgnoreCase("src")) {
                if (!incRule.isEmpty()) {
                    if (!data.toLowerCase().contains("where")) {
                        data = data + " where " + incRule;
                    } else {
                        String tmpQry = data.substring(0, data.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + data.substring(data.lastIndexOf("where") + 5, data.length());
                        data = tmpQry;
                    }
                }
                qry = "select sum(" + tblname + "." + sqlParser.getColumnNamefromQuery(data) + ") as " + sqlParser.getColumnNamefromQuery(data) + " from (" + data + ")" + tblname;
            } else {
                if (!trgRule.isEmpty()) {
                    if (!trgRule.toLowerCase().contains("where")) {
                        qry = "select sum(" + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + "  where " + trgRule;
                    } else {
                        qry = "select sum(" + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname + "  " + trgRule;
                    }
                } else {
                    qry = "select sum(" + tblname + "." + data.trim() + ") as " + data.trim() + " from " + schema + "." + tblname + " " + tblname;
                }
                if (!incRule.isEmpty()) {
                    if (!qry.toLowerCase().contains("where")) {
                        qry = qry + " where " + incRule;
                    } else {
                        String tmpQry = qry.substring(0, qry.toLowerCase().lastIndexOf("where") + 5) + " " + incRule + " and " + qry.substring(qry.lastIndexOf("where") + 5, qry.length());
                        qry = tmpQry;
                    }

                }
            }
        } else {

            if (hostType.equalsIgnoreCase("trg")) {
                if (trgRule.isEmpty()) {
                    qry = "select sum(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\"";
                } else {
                    if (!trgRule.toLowerCase().contains("where")) {
                        qry = "select sum(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\" where " + trgRule;
                    } else {
                        qry = "select sum(" + data.trim() + ") as " + data.trim() + " from \"" + tblname + "\"  " + trgRule;
                    }
                }
            } else {
                String tmp = data.trim().replaceAll("/", ".");

                if (trgRule.isEmpty()) {
                    qry = "select sum(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\"";
                } else {
                    if (!trgRule.toLowerCase().contains("where")) {
                        qry = "select sum(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\" where " + trgRule;
                    } else {
                        qry = "select sum(" + sqlParser.getColumnNamefromQuery(tmp) + ") as " + sqlParser.getColumnNamefromQuery(tmp) + " from  \"" + tblname + "\"  " + trgRule;
                    }
                }
            }
        }
        LOGGER.info("Total Sum Query Generated ");

        return qry;

    }

    public boolean validateSpecialConditon(String data) {

        boolean flag = false;

        String[] notallowed = {"count", "sum", "max", "min", "avg"};

        for (String item : notallowed) {

            if (data.toLowerCase().contains(item)) {
                flag = false;
                break;
            } else {
                flag = true;
            }

        }

        return flag;
    }

//    public static String checkColType(String conn, String colName, String tableName) throws ClassNotFoundException, Exception {
//        //Processing the where condition 
//
//        CSVSQLEngine cSVSQLEngine = new CSVSQLEngine();
//        String colType = cSVSQLEngine.getFFColumnType(new File(conn).getParent() + "?schema=C:/Users/Admin/Desktop/schema3.xml", colName, tableName, true);
//
//        if (colType.toLowerCase().contains("varchar")) {
//            return colName;
//        }
//
//        return "char(" + colName + ")";
//    }

}
