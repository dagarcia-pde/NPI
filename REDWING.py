import pandas as pd
import numpy as np  
import sys

import PyUber

class RedWing:
    def __init__(self, *args, **kwargs):
        """Initialize the framework with parameters."""
        
        self.xeus_source = kwargs.get('xeus_source', 'F32_PROD_XEUS')  # Access 'xeus_source' keyword argument
        self.debug_flag = kwargs.get('debug_flag', False)  # Access 'xeus_source' keyword argument
        
        self.rawRW = self.extractRW()# Example of using kwargs
        if self.debug_flag: self.rawRW.to_csv(r'debug\rawRW.csv', index=False)
        
        self.NPI_List = self.extract_info()
        if self.debug_flag: self.NPI_List.to_csv(r'debug\NPI_List.csv', index=False)

    def sql_query(self):
        query = '''
            Select DISTINCT
                RWD.DEPT_NAME,
                RWG.GROUP_NAME,
                RWLM.DOTPROCESS,
                RWLG.SCENARIO_NAME,
                FL.LOT_TITLE,
                RWLG.LOT,
                RWLM.OPERATION || ' ' || RWLM.OPER_SHORT_DESC,
                RWLS.SEGMENT_DAY AS SEG_DAY,
                RWSS.HOTBOX AS HB,
                RWLS.COMMITOUT AS COMMIT_OUT,
                RWLS.EXPECTEDOUT AS ETA,
                RWLM.HAO as HAO
                
            FROM
                F_RW_DEPT RWD,
                F_RW_GROUP RWG,
                F_RW_LOT_GROUP RWLG,
                F_RW_LOT_MASTER RWLM,
                F_RW_LOT_SCENARIO RWLS,
                F_RW_SCENARIO_SUM RWSS,
                F_LOT FL
            WHERE
                RWD.DEPT_NAME LIKE '%NPI%'
                AND RWD.DEPT_ID = RWG.DEPT_ID
                AND RWLG.GROUP_ID = RWG.GROUP_ID
                AND RWLG.UPDATED_BY NOT LIKE '%ATC%'
                AND RWLG.LOT = RWLM.LOT
                AND RWLG.LOT = RWLS.LOT
                AND RWLG.LOT = RWSS.LOT
                AND RWLG.LOT = FL.LOT
                AND RWLG.SCENARIO_NAME = RWLS.SCENARIO_NAME
                AND RWLG.SCENARIO_NAME = RWSS.SCENARIO_NAME
                
            Order By
                RWD.DEPT_NAME desc,
                RWG.GROUP_NAME,
                RWLG.SCENARIO_NAME
        '''
        
        return query
    
    def extractRW(self):
        with PyUber.connect(datasource=self.xeus_source) as conn:
            query = self.sql_query()
            df = pd.read_sql(query, conn)
        return df

    def determine_lot_type(self, lot_title, scenario_name):
        
        ss_type = ''
        # Check for skip scout
        if 'EF' in lot_title:
            ss_type = 'EF '
        elif 'SC' in lot_title and 'SCOUT' not in lot_title:
            ss_type = 'SC '
        
        if 'CHILD' in scenario_name:
            return f'{ss_type}Child Lot'
        elif any(keyword in lot_title for keyword in ['SCOUT1', 'S1', 'SCOUT 1']):
            return f'{ss_type}Scout 1'
        elif any(keyword in lot_title for keyword in ['SCOUT2', 'S2', 'SCOUT 2']):
            return f'{ss_type}Scout 2'
        elif any(keyword in lot_title for keyword in ['SILENT LOT', 'SILENTLOT', ' SL']):
            return 'Silent Lot'
        elif any(keyword in lot_title for keyword in ['LEAD', ' LL']):
            return 'Lead Lot'
        elif any(keyword in lot_title for keyword in ['FO', 'FOLLOW']):
            return 'Follow On Lot'
        elif any(keyword in lot_title for keyword in ['CQ', 'CROSS']):
            return 'Cross Qual'
        elif 'BULL' in lot_title:
            return 'Bull'
        else:
            return None  # Return None or a default color if no keywords match
    
    def extract_info(self):
        self.NPIs = self.rawRW['GROUP_NAME'].unique()
        
        extracted_info = []
        
        for _, row in self.rawRW.iterrows():
            group_name = row['GROUP_NAME']
            dotprocess = row['DOTPROCESS']
            lot_title = row['LOT_TITLE'] 
            scenario_name = row['SCENARIO_NAME'] 
            lot = row['LOT'] 
            commit = row['COMMIT_OUT']
            lot_type = self.determine_lot_type(lot_title, scenario_name)
            
            if lot_type is not None:
                
            
                extracted_info.append({
                    'GROUP_NAME': group_name,
                    'DOTPROCESS': dotprocess,
                    'LOT_TITLE': lot_title,
                    'LOT': lot,
                    'LOT_TYPE': lot_type,
                    'COMMIT': commit
                })
                
        df = pd.DataFrame(extracted_info)
        return df
    
    def get_lots(self, GROUP_NAME):
        """Get lots for a specific group name."""
        if GROUP_NAME not in self.NPIs:
            raise ValueError(f"Group name '{GROUP_NAME}' not found in NPI list.")
        
        # lots = self.NPI_List[self.NPI_List['GROUP_NAME'] == GROUP_NAME]['LOT'].unique()
        lots_with_types = self.NPI_List[self.NPI_List['GROUP_NAME'] == GROUP_NAME][['LOT', 'LOT_TYPE','COMMIT']].drop_duplicates()
        lots = lots_with_types.to_dict('records')
        return lots
# RW = RedWing(xeus_source='F32_PROD_XEUS', debug_flag=True)


