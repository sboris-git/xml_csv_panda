import datetime

import pandas as pd
import xml.etree.ElementTree as et


def xml_pd_parse():
    begin_time = datetime.datetime.now()
    # xml data lineitem.xml from
    # http://aiweb.cs.washington.edu/research/projects/xmltk/xmldata/www/repository.html#nasa
    xtree = et.parse('data/lineitem.xml')
    xroot = xtree.getroot()
    df_cols = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
               'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS',
               'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'
               ]
    # <L_ORDERKEY> 1</L_ORDERKEY>
    # <L_PARTKEY> 1552</L_PARTKEY>
    # <L_SUPPKEY> 93</L_SUPPKEY>
    # <L_LINENUMBER> 1</L_LINENUMBER>
    # <L_QUANTITY> 17</L_QUANTITY>
    # <L_EXTENDEDPRICE> 24710.35</L_EXTENDEDPRICE>
    # <L_DISCOUNT> 0.04</L_DISCOUNT>
    # <L_TAX> 0.02</L_TAX>
    # <L_RETURNFLAG> N</L_RETURNFLAG>
    # <L_LINESTATUS> O</L_LINESTATUS>
    # <L_SHIPDATE> 1996-03-13</L_SHIPDATE>
    # <L_COMMITDATE> 1996-02-12</L_COMMITDATE>
    # <L_RECEIPTDATE> 1996-03-22</L_RECEIPTDATE>
    # <L_SHIPINSTRUCT> DELIVER IN PERSON</L_SHIPINSTRUCT>
    # <L_SHIPMODE> TRUCK</L_SHIPMODE>
    # <L_COMMENT> blithely regular ideas caj</L_COMMENT>

    out_df = pd.DataFrame(columns=df_cols)
    i = 0
    for node in xroot:
        raw = []

        for col in df_cols:
            s_name = node.attrib.get(col)
            raw.append(node.find(col).text if node is not None else None)
        # ToDo to set a limit of records to be processed - uncomment if block and change an integer
        if i > 1000:
            break
        out_df = out_df.append(pd.Series(raw, index=df_cols),
                               ignore_index=True)
        i += 1

    # print(out_df)
    out_df.to_csv('test.csv', index=False, header=True)
    print(f"Processed {i} records in total 30 MB in {datetime.datetime.now() - begin_time} "
          f"- hour:minute:second:microsecond")


if __name__ == '__main__':
    xml_pd_parse()
