import pandas as pd
import json


class TrafficDataHandler:
    def __init__(self, file_name):
        self.df = pd.read_excel(file_name)
        self.date = []
        self.period_from = []
        self.period_to = []
        self.district_data = {}
        self.per_district_data_frames = {}
        self.merge_data_frames = {}
        self.per_hour_data_frames = {}

    def convertToDF(self):
        return self.df

    def getColumnHeaders(self):
        return self.df.columns.tolist()

    def fillDate(self):
        cur_date = None

        for date in self.df["Date"]:
            if not pd.isna(date):
                cur_date = date
            self.date.append(cur_date)

    def fillPeriod(self):
        cur_period_from = None
        cur_period_to = None

        for period_from in self.df["Period_from"]:
            if not pd.isna(period_from):
                cur_period_from = period_from
            self.period_from.append(cur_period_from)

        for period_to in self.df["Period_to"]:
            if not pd.isna(period_to):
                cur_period_to = period_to
            self.period_to.append(cur_period_to)

    def extractDistrict(self, district, detector_id):
        self.district_data[district] = {"Date": self.date, "Period_from": self.period_from, "Period_to": self.period_to}
        for aid in detector_id:
            if aid in self.df.columns.tolist():
                data = {'speed': [], 'volume': []}
                for entry in self.df[str(aid)]:
                    if not pd.isna(entry):
                        entry_dict = json.loads(entry.replace("'", '"'))
                        data['speed'].append(round(entry_dict['average_speed'], 2))
                        data['volume'].append(round(entry_dict['average_volume'], 2))
                    else:
                        data['speed'].append('-')
                        data['volume'].append('-')
                self.district_data[district][f"{aid}.speed"] = data['speed']
                self.district_data[district][f"{aid}.volume"] = data['volume']

    def perDistrictDataFrame(self, district):
        self.per_district_data_frames[district] = pd.DataFrame(self.district_data[district])
        self.per_district_data_frames[district].to_csv(f"extracted_data/Separated/{district}_{self.date[0][0:7]}_{self.date[-1][0:7]}.csv", index=True)
        print(f"Successfully export the separated {district} dataframe")

    def mergePerDistrict(self, district):
        df = self.per_district_data_frames[district]
        id_vol = []
        id_speed = []
        new_cols_data = {'Speed': [], 'Volume': []}
        for item in df:
            if "volume" in item:
                id_vol.append(item)
            if "speed" in item:
                id_speed.append(item)
        for ind in df.index:
            avg_vol, avg_speed = 0, 0
            count_vol, count_speed = 0, 0
            for aid in id_vol:
                if isinstance(df[aid][ind], float):
                    avg_vol += df[aid][ind]
                    count_vol += 1
            for aid in id_speed:
                if isinstance(df[aid][ind], float):
                    avg_speed += df[aid][ind]
                    count_speed += 1
            if avg_vol == 0 or avg_speed == 0:
                new_cols_data['Speed'].append('-')
                new_cols_data['Volume'].append('-')
            else:
                new_cols_data['Speed'].append(round(avg_speed/count_speed, 2))
                new_cols_data['Volume'].append(round(avg_vol/count_vol, 2))
        new_columns_df = pd.DataFrame(new_cols_data)
        result_df = pd.concat([df, new_columns_df], axis=1)
        result_df = result_df.drop(id_vol+id_speed, axis=1)
        result_df.to_csv(f"extracted_data/Merge_to_Average/{district}_{self.date[0][0:7]}_{self.date[-1][0:7]}.csv", index=True)
        self.merge_data_frames[district] = result_df
        print(f"Successfully export the merged average {district} dataframe")

    def mergeTime(self, district):
        if len(self.district_data[district]) > 3:
            self.perDistrictDataFrame(district)
            self.mergePerDistrict(district)
            result = {"Date": [], "Period": [], "Avg_Speed": [], "Avg_Volume": []}
            prev_date = None
            for ind in self.merge_data_frames[district].index:
                if isinstance(self.merge_data_frames[district].iloc[ind]["Speed"], float) and \
                        isinstance(self.merge_data_frames[district].iloc[ind]["Volume"], float):
                    if self.merge_data_frames[district].iloc[ind]["Date"] != prev_date:
                        prev_date = self.merge_data_frames[district].iloc[ind]["Date"]
                        prev_period = f"{self.merge_data_frames[district].iloc[ind]['Period_from'][0:2]}:00"
                        result["Date"].append(prev_date)
                        result["Period"].append(prev_period)
                        result["Avg_Speed"].append(self.merge_data_frames[district].iloc[ind]["Speed"])
                        result["Avg_Volume"].append(self.merge_data_frames[district].iloc[ind]["Volume"])
                    else:
                        if prev_period == f"{self.merge_data_frames[district].iloc[ind]['Period_from'][0:2]}:00":
                            result["Avg_Speed"][-1] = round((result["Avg_Speed"][-1] +
                                                                       self.merge_data_frames[district].iloc[ind]["Speed"])/2, 2)
                            result["Avg_Volume"][-1] = round((result["Avg_Volume"][-1] +
                                                                        self.merge_data_frames[district].iloc[ind]["Volume"])/2, 2)
                        else:
                            prev_period = f"{self.merge_data_frames[district].iloc[ind]['Period_from'][0:2]}:00"
                            result["Date"].append(prev_date)
                            result["Period"].append(prev_period)
                            result["Avg_Speed"].append(self.merge_data_frames[district].iloc[ind]["Speed"])
                            result["Avg_Volume"].append(self.merge_data_frames[district].iloc[ind]["Volume"])
            self.per_hour_data_frames[district] = pd.DataFrame(result)
            self.per_hour_data_frames[district].to_csv(f"extracted_data/Per_Hour/{district}_{self.date[0][0:7]}_{self.date[-1][0:7]}.csv", index=True)
            print(f"Successfully export the per hour {district} dataframe")
        else:
            print(f"{district} has no data")

    def mergeTwoFiles(self, file1, file2, merge_att, output_file):
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)
        merged_df = pd.merge(df1, df2, on=merge_att, how='outer')
        merged_df.sort_values(by=['Date', 'Period'], inplace=True)
        merged_df.dropna(axis=0, how='any', inplace=True)
        merged_df.to_csv(output_file, index=False)

    def mergeXwithY(self):
    aqhi_file = pd.ExcelFile("AQHI.xlsx")
    sheet_names = aqhi_file.sheet_names

    for sheet_name in sheet_names:
        try:
            aqhi_df = aqhi_file.parse(sheet_name)
            aqhi_df["Date"] = pd.to_datetime(aqhi_df['Date'], errors='coerce')
            aqhi_df["Period"] = aqhi_df['Period'].astype(str)
            print(sheet_name)
            x_df = pd.read_csv(f"Weather&Traffic/{sheet_name}.csv")
            x_df["Date"] = pd.to_datetime(x_df['Date'], errors='coerce')
            x_df["Period"] = x_df['Period'].astype(str)

            x_df = pd.merge(x_df, aqhi_df, on=["Date", "Period"])

            x_df.dropna(axis=0, how='any', inplace=True)

            x_df.to_csv(f"FinalData/{sheet_name}.csv")
            print(f"Successfully x and y variable for {sheet_name}")
        except Exception as e:
            print(e)
