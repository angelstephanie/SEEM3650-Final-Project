import pandas as pd
import requests
import xml.etree.ElementTree as ET
import datetime


def get_date_input():
    while True:
        date_str = input("Enter a date in the format YYYYMMDD: ")
        try:
            date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
            return date
        except ValueError:
            print("Invalid date format. Please try again.")


def loadRSS(url):
    resp = requests.get(url)
    with open("trafficSpeed.xml", "wb") as f:
        f.write(resp.content)


def parseXML(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    data_dict = {}
    period_ls = []

    for period in root.findall('.//period'):
        date = root.find('date').text
        period_from = period.find('period_from').text
        period_to = period.find('period_to').text
        period_ls.append(period_from)
        period_ls.append(period_to)

        detectors = period.findall('.//detector')
        for detector in detectors:
            detector_id = detector.find('detector_id').text
            lanes = detector.findall('.//lane')

            total_speed = 0
            total_volume = 0
            lane_count = 0

            for lane in lanes:
                speed = int(lane.find('speed').text)
                volume = int(lane.find('volume').text)
                total_speed += speed
                total_volume += volume
                lane_count += 1

            average_speed = total_speed / lane_count
            average_volume = total_volume / lane_count

            if date not in data_dict:
                data_dict[date] = {}

            if (period_from, period_to) not in data_dict[date]:
                data_dict[date][(period_from, period_to)] = {}

            data_dict[date][(period_from, period_to)][detector_id] = {
                'average_speed': average_speed,
                'average_volume': average_volume
            }

    # Combine periods and calculate average speed and volume
    combined_data_dict = {}

    for date, periods_data in data_dict.items():
        combined_data_dict[(date, period_ls[0], period_ls[-1])] = {}

        for detector_data in periods_data.values():
            for detector_id, values in detector_data.items():
                if detector_id not in combined_data_dict[(date, period_ls[0], period_ls[-1])]:
                    combined_data_dict[(date, period_ls[0], period_ls[-1])][detector_id] = {
                        'average_speed': 0,
                        'average_volume': 0,
                        'period_count': 0
                    }

                combined_data_dict[(date, period_ls[0], period_ls[-1])][detector_id]['average_speed'] += values['average_speed']
                combined_data_dict[(date, period_ls[0], period_ls[-1])][detector_id]['average_volume'] += values['average_volume']
                combined_data_dict[(date, period_ls[0], period_ls[-1])][detector_id]['period_count'] += 1

        for detector_id, values in combined_data_dict[(date, period_ls[0], period_ls[-1])].items():
            values['average_speed'] /= values['period_count']
            values['average_volume'] /= values['period_count']

    return combined_data_dict


# Get initial and ending dates from the user
print("Enter the initial date:")
start_date = get_date_input()

print("\nEnter the ending date:")
end_date = get_date_input()

# Generate the list of dates
date_list = []
current_date = start_date
while current_date <= end_date:
    date_list.append(current_date.strftime("%Y%m%d"))
    current_date += datetime.timedelta(days=1)

# Print the list of dates
print("\nList of dates between", start_date.strftime("%Y%m%d"), "and", end_date.strftime("%Y%m%d"), ":")
print(date_list)


datetime = []
time = []

for i in range(0, 24):
    time.append(f"{str(i).zfill(2)}00")
    time.append(f"{str(i).zfill(2)}30")

for date in date_list:
    for t in time:
        datetime.append(f"{date}-{t}")

xml_link_prefix = "https://api.data.gov.hk/v1/historical-archive/get-file?url=https%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Ftraffic-detectors%2FrawSpeedVol-all.xml&time="

trafficSpeedVolume = []

for date in datetime:
    loadRSS(xml_link_prefix + date)
    try:
        trafficSpeedVolume.append(parseXML("trafficSpeed.xml"))
    except:
        print("error in ", date)
    print(date)

filename = "combined_data_.xlsx"
first_df = pd.DataFrame.from_dict(trafficSpeedVolume[0])
first_df = first_df.transpose()
i = 1

for data in trafficSpeedVolume:
    print(i)
    i += 1
    new_df = pd.DataFrame.from_dict(data)
    new_df = new_df.transpose()

    first_df = pd.concat([first_df, new_df])

first_df.to_excel(filename, index=True)
