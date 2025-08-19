import pandas as pd
import numpy as np  
import sys

import PyUber
import threading

class Product:
    def __init__(self, *args, **kwargs):

        self.query_count = 1

        self.define_modules()  # Call the method to define modules
        self.litho_operation_decoder()
        
        self.lot_dict = {}
        self.LotFlows = pd.DataFrame()
        
        self.xeus_source = kwargs.get('xeus_source', 'F32_PROD_XEUS')  # Access 'xeus_source' keyword argument
        self.ret_xeus_source = kwargs.get('ret_xeus_source', 'D1D_PROD_XEUS_CENTRAL')  # Access 'xeus_source' keyword argument
        
        self.ret_overide = kwargs.get('ret_overide', 'ReticleConfig.xlsx')  # Access 'xeus_source' keyword argument
        
        self.debug_flag = kwargs.get('debug_flag', False)  # Access 'debug_flag' keyword argument
        self.verbose = kwargs.get('verbose', False)  # Access 'debug_flag' keyword argument
        self.npi = kwargs.get('npi', None)  # Access 'xeus_source' keyword argument
        self.product = kwargs.get('product', None)  # Access 'xeus_source' keyword argument

        if self.product is None:
            raise ValueError("Product must be specified")
        elif self.npi is None:
            raise ValueError("NPI must be specified")
        else:
            df = self.run_query(self.sql_LeadLot_query(),self.xeus_source)
            lot7 = ','.join([f"'{lot7}'" for lot7 in df['LOT7'].unique()])
            self.master_flow = self.masterFlow(self.run_query(self.sql_lot_query(lot7,7),self.xeus_source))
            
            self.retDataRaw = self.run_query(self.sql_Reticle_query(self.product, self.product[:5]+self.product[-1:]), self.ret_xeus_source)      
            self.retConfig = pd.read_excel(self.ret_overide, sheet_name='ReticleConfig')
            self.Reticles = self.reticle_cleanup()
            
            
    def litho_operation_decoder(self):
        self.cond_list = [' ','#',
                    'L58','L5B','L52','L46','L4H','L4','L5', #10nm conditions
                    'L8xr','L8c','L8s','L8b','L86','L81','L8d','L8' #18A conditions
                    ]         
    def define_modules(self):
        # Define module lists
        modules_18A = [
            "LI-SAVli", "LI-SAYli", "LI-SBHcu", "LI-SBLcu", "LI-SNEli", "LI-SNYli"
        ]

        modules_10nm = [
            "LI-BE-193", "LI-BE-SED", "LI-BE-WET", "LI-FE-193", "LI-PD-WET",
            "LI-SSAFI-WET", "LI-WET", "LI-FE-248"
        ]        
        
        self.modules = modules_18A + modules_10nm
        
    def sql_LeadLot_query(self):
        query = f'''
            SELECT DISTINCT
                LOT7
            FROM
                F_LOT
            WHERE
                PRODUCT LIKE '{self.product}'
                AND LOT_TITLE LIKE 'NPI% LL%'
        '''
        return query
    
    def sql_lot_query(self, lot, lot_length=8):
        
        print (f"Running master query for LOT: {lot}")
        
        sql_adder = f'LOT IN ({lot})'
        if lot_length == 7:
            sql_adder = f'LOT7 IN ({lot})'
            
        # SQL query to get lot information
        query = f'''
            SELECT DISTINCT
                lf.LOT
                ,lf.OPERATION
                ,lf.OPER_SHORT_DESC AS OPER_SHORT
                ,o.oper_long_desc AS OPER_LONG
                ,o.area AS AREA
                ,o.module AS MODULE
                ,MIN(lf.EXEC_SEQ) AS SEQ
                ,max(lf.out_date) as OUT_DATE
            FROM
                F_LOT_FLOW lf
                CROSS JOIN F_Facility f
                INNER JOIN F_Operation O ON o.operation=lf.operation AND o.facility = f.facility AND o.latest_version = 'Y'
            WHERE
                {sql_adder}
                AND LENGTH(lf.LOT) = 8
            GROUP BY
                lf.lot
                ,lf.OPERATION
                ,lf.OPER_SHORT_DESC
                ,o.oper_long_desc
                ,o.area
                ,o.module
        '''
        return query
    
    def sql_Reticle_query(self,fab_prod,ret_name,imo_fab_list="'F32','F42','F12','F22','F52'"):
        query = f'''
            SELECT 
                z0.commonname AS common_name
                ,z0.title AS title
                ,'{fab_prod}' AS FAB_PROD
                ,z0.product AS RET_PROD
                ,z0.rev AS rev
                ,z0.layer AS layer
                ,z0.step AS step
                ,z0.platetype AS plate_type
                ,z0.tapeintrend AS tapein_trend
                ,z0.itotrend AS ito_trend
                ,To_Char(z0.itocommit,'yyyy-mm-dd hh24:mi:ss') AS ito_commit
                ,z0.itostatus AS ito_status
                ,z0.retfabrev AS ret_fabrev
                ,z0.fab AS fab
                ,z0.barcode AS barcode
                ,z0.imotrend AS imo_trend
                ,To_Char(z0.imocommit,'yyyy-mm-dd hh24:mi:ss') AS imo_commit
                ,z0.imostatus AS imo_status
                ,To_Char(z0.shipdate,'yyyy-mm-dd hh24:mi:ss') AS shipdate
                ,z0.fabrequireddate AS fab_requireddate
                ,z0.imodotprocess AS imo_dotprocess
                ,z0.imoishot AS imo_ishot
                ,z0.technology AS technology
                ,z0.toengcontact AS to_engcontact
                ,z0.dbnames AS dbnames
                ,To_Char(z0.last_updated_timestamp,'yyyy-mm-dd hh24:mi:ss') AS last_updated_timestamp
            FROM 
                F_IMO_TRIFECTA_DASHBOARD z0
            WHERE
                1=1
                --AND z0.last_updated_timestamp >= SYSDATE - 180 
                AND z0.fab In ({imo_fab_list})     
                AND (
                    z0.product LIKE '{ret_name}'
                )
        '''
        return query
    
    def run_query(self, query, xeus_source):

        if self.verbose: print(f"SQL Query {self.query_count}: {query}")
        
        with PyUber.connect(datasource=xeus_source) as conn:
            df = pd.read_sql(query, conn)
        
        if self.debug_flag:
            filename = f'debug\\{self.product}_query_{str(self.query_count)}.csv'
            self.query_count += 1
            df.to_csv(filename, index=False)
        
        return df
    
    def get_layer(self, row):
        
        if row['OPER_LONG'].find('START') != -1: return 'START'
        if row['OPER_LONG'].find('PACK') != -1: return 'SHIP'
        
        value = row['OPER_SHORT']

        for cond in self.cond_list:
            value = value.replace(cond,'')
        
        value = value[:3]

        if row['OPER_LONG'].find(value) == -1:
            if value[0]=='M':
                value = 'MT' + value[1]
            else:
                value = 'VA' + value[1]    
        
        return value
    
    def masterFlow(self, df):
        df3 = df.copy() 
        df3 = df3.sort_values(by='SEQ').reset_index(drop=True)
        df3['PC_STARTS_flag'] = False
        df3['OPERATION_flag'] = False
        # df3['AREA_flag'] = False
        df3['MODULE_flag'] = False
        df3.loc[df3['MODULE'] == "PC-STARTS", 'PC_STARTS_flag'] = True
        df3.loc[df3['OPERATION'] == 9812, 'OPERATION_flag'] = True
        # df3.loc[df3['AREA'] == 'LITHO', 'AREA_flag'] = True
        df3.loc[df3['MODULE'].isin(self.modules), 'MODULE_flag'] = True

        df3 = df3[df3[['PC_STARTS_flag', 'OPERATION_flag', 'MODULE_flag']].any(axis=1)].reset_index(drop=True)


        df3['ORDER'] = range(len(df3))
        df3 = df3.drop(columns=['PC_STARTS_flag', 'OPERATION_flag', 'MODULE_flag','SEQ','MODULE','AREA'])
        df3['LAYER'] = df3.apply(lambda row: self.get_layer(row), axis=1 )

        df3 = df3[['ORDER', 'OPERATION', 'OPER_SHORT', 'OPER_LONG', 'LAYER']]
        
        return df3
    
    def load_lot_list(self, lot_list):
        
        threads = []
        for lot in lot_list:
            print(f"LOT: {lot['LOT']}, LOT_TYPE: {lot['LOT_TYPE']}")
            if lot['LOT'] not in self.lot_dict:
                thread = threading.Thread(target=self.add_Lot, args=(lot['LOT'], lot['LOT_TYPE'], lot.get('COMMIT')))
                threads.append(thread)
                thread.start()
                # self.add_Lot(lot['LOT'], lot['LOT_TYPE'], lot['COMMIT'])
        
        
        for thread in threads:
            thread.join()
            
    def add_Lot(self, lot,lot_type, commit=None):
        df = self.run_query(self.sql_lot_query(f"'{lot}'"), self.xeus_source)
        df['LOT_TYPE'] = lot_type
        df['NPI'] = self.npi
        df['COMMIT'] = commit
        mf = self.master_flow
        df2 = pd.merge(df[['NPI','LOT_TYPE','LOT','COMMIT','OPERATION','OPER_SHORT','OPER_LONG','OUT_DATE']], mf[['OPER_SHORT','ORDER','LAYER']], left_on=['OPER_SHORT'], right_on=['OPER_SHORT'], how='inner').sort_values(by='ORDER').reset_index(drop=True)

        commit = df2.iloc[0]['COMMIT']
        release_date = df2.iloc[0]['OUT_DATE']
        reticle_layers = df2['ORDER'].max()

        TPT = (commit - release_date).days
        DPML = TPT/reticle_layers

        df2['DPML_CUM'] = df2['ORDER'].apply(lambda x: DPML * (x))
        df2['PLAN'] = release_date + pd.to_timedelta(df2['DPML_CUM'], unit='D')

        df2 = df2.drop('DPML_CUM', axis=1)
        
        self.lot_dict[lot] = df2
        
        if lot not in self.LotFlows:
            self.LotFlows = pd.concat([self.LotFlows, df2.assign(LOT=lot)], ignore_index=True)
        
    def cleanRetCol(self,dt):
        try:
            dt = pd.to_datetime(dt)
            return dt
        except ValueError:
            dt = dt.str.replace('~','')
            dt = pd.to_datetime(dt)
            return dt
        
    def convert_to_days(self,col,min_val):
        col = col - min_val
        col = col.apply(lambda x: x.days if x>pd.Timedelta(days=0) else 0)
        return col

    def cleanupCommit(self,row):
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=180)
        
        if row['IMO_TREND'] < cutoff_date:
            return row['IMO_COMMIT']
        else:
            return row['IMO_TREND']    
        
    def reticle_version_handling(self,df, ret_version):
        """
        Uses the reticle data version input to overide and select specific reticles to use in tracking
        """
        # df['VER'] = df['RET_PROD'].str[:3]
        df['VER'] = df['TITLE'].str[:3]
        merged_data = df.merge(ret_version, how='left', on=['RET_PROD', 'LAYER'])
        df = merged_data[(merged_data['VER'] == merged_data['VERSION']) | merged_data['VERSION'].isna()]
        df.drop(columns=['VER','VERSION'], inplace=True)
        
        return df
    
    def reticle_cleanup(self):
        RetData = self.retDataRaw.copy()
        col_list =['TAPEIN_TREND','ITO_TREND','ITO_COMMIT','IMO_TREND','IMO_COMMIT','SHIPDATE','FAB_REQUIREDDATE']
        for col in col_list:
            RetData[col] = self.cleanRetCol(RetData[col])
        if self.retConfig is not None:
            RetData = self.reticle_version_handling(RetData, self.retConfig)
        RetData['IMO_TREND'] = RetData.apply(lambda row: self.cleanupCommit(row), axis =1)
        indexNames = RetData[RetData['IMO_STATUS']=='Rejected'].index
        RetData.drop(indexNames, inplace=True)

        indexNames = RetData[RetData['IMO_STATUS']=='Processing - Hold With Waiver'].index
        RetData.drop(indexNames, inplace=True)

        RetData['RetRev'] = RetData['TITLE'].str.slice(0, 3) 
        RetData['RetNum'] = RetData['TITLE'].str[3:4]   
        shipped_data = RetData[RetData['IMO_STATUS'] == 'Shipped'][['FAB_PROD', 'LAYER']].drop_duplicates()
        RetData = RetData[~((RetData['IMO_STATUS'] != 'Shipped') & (RetData['LAYER'].isin(shipped_data['LAYER'])) & (RetData['RET_PROD'].isin(shipped_data['FAB_PROD'])))]

        RetData = RetData.sort_values(by='IMO_COMMIT').drop_duplicates(subset=['LAYER'], keep='first')
        RetData = pd.pivot_table(RetData,index=['LAYER'], values=['TAPEIN_TREND','ITO_TREND','FAB_REQUIREDDATE','IMO_TREND','SHIPDATE'], aggfunc=np.min).reset_index()        
        
        return RetData