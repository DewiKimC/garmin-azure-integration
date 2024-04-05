import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime as dt
import os
import shutil
import json
import pytz
import math
from azure.storage.blob import BlobServiceClient

class Garmin:
    def __init__(self, account_url, sas_token):
        self.blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)

    def unzipGarminFile(self, inputBlob, tempLocalPath):
        with ZipFile(inputBlob, 'r') as zipObj:
            zipObj.extractall(tempLocalPath)

    def unzipSubject(self, subjectID, containerSubject="participants"):
        # This gets all the blob names from Azure
        container_client = self.blob_service_client.get_container_client(container=containerSubject)

        # Lists all the blobs currently in "testParticipant01"
        start_string = subjectID + "/sensordata/sync"
        blob_names_list = list(container_client.list_blob_names(name_starts_with=start_string))
        print("Blob names: ", blob_names_list)
        print("Last Blob:  ", blob_names_list[-1])

        # This lists all the blobs (zip files) in participants/sensorsdata/sync*....*.zip
        blob_list = container_client.list_blobs(start_string)
        latest_blob = sorted(blob_list, key=lambda blob: blob.creation_time)[-1] #sorteert de lijst blobnames list op de eerste element van elk tuple en neemt hierna de eerste in de array
        print("last updated: ", latest_blob)

        temporary_local_path = os.getcwd() + "/" + subjectID
        os.makedirs(temporary_local_path)
        for blob_name in blob_names_list:
            blob_client = self.blob_service_client.get_blob_client(container=containerSubject, blob=blob_name)  # This is sort of a reference to the blob data on Azure
            with BytesIO() as input_blob:  # Writes data to a temporary file
                # This is old and deprecated function to download the blob.
                # blob_client.download_blob().download_to_stream(input_blob=input_blob)
                blob_client.download_blob().readinto(input_blob)

                input_blob.seek(0)  # This can, I think, be removed...
                self.unzipGarminFile(input_blob, temporary_local_path)
            datafolder = os.listdir(temporary_local_path)[0]
            folder_path = temporary_local_path + "/" + datafolder
            datafile_list = os.listdir(folder_path)
            for datafile in datafile_list:
                file_path = folder_path + "/" + datafile
                datafile_name = "/" + datafile
                new_blob_name = blob_name.replace("sensordata", "garmindata")
                new_blob_name = new_blob_name.replace(".zip", datafile_name)
                # azure.uploadBlobFile(container_client, file_path, new_blob_name)
                with open(file=file_path, mode="rb") as data:
                    container_client.upload_blob(name=new_blob_name, data=data, overwrite=True)
            shutil.rmtree(folder_path)
        shutil.rmtree(temporary_local_path)

    def unzipGroup(self, subjectList):
        for subject in subjectList:
            self.unzipSubject(subject)

    def getOperatingSystem(self, blobName):
        filename = blobName.split('/')[-1]
        if filename[0].isnumeric():
            return "android"
        elif not filename[0].isnumeric():
            return "ios"
        else:
            return "unknown_os"

    def readBodyBatteryIos(self, inputBlob):
        bbData = json.load(inputBlob)
        checks = ['value', 'description']
        tsList = [i['timestamp'] for i in bbData if all([x in i for x in checks])]
        valueList = [i['value'] for i in bbData if all([x in i for x in checks])]
        descriptionList = [i['description'] for i in bbData if all([x in i for x in checks])]
        statusListRaw = [i.split("status: ")[-1] for i in descriptionList]
        statusListRaw = [i.split(" ")[0] for i in statusListRaw]
        if any(["\n" in i for i in statusListRaw]):
            statusList = [i.replace("\n", "") for i in statusListRaw if "\n" in i]
        else:
            statusList = statusListRaw
        bodyBatteryDf = pd.DataFrame(list(zip(tsList, statusList, valueList)),
                                     columns=["timestamp", "body_battery_status", "body_battery_score"])
        return bodyBatteryDf

    def readMotionIos(self, inputBlob):
        motionData = json.load(inputBlob)
        checks = ['duration', 'intensity']
        tsList = [i['startTime'] for i in motionData if all([x in i for x in checks])]
        durationList = [i['duration'] for i in motionData if all([x in i for x in checks])]
        intensityList = [i['intensity'] for i in motionData if all([x in i for x in checks])]
        descriptionList = [i['description'] for i in motionData if all([x in i for x in checks])]
        activityListRaw = [i.split("activityType: ")[-1] for i in descriptionList]
        activityListRaw = [i.split(" ")[0] for i in activityListRaw]
        if any(["\n" in i for i in activityListRaw]):
            activityList = [i.replace("\n", "") for i in activityListRaw if "\n" in i]
        else:
            activityList = activityListRaw
        stepsList = [i.split("hasSteps: ")[-1] for i in descriptionList]
        motionDf = pd.DataFrame(list(zip(tsList, durationList, intensityList, activityList, stepsList)),
                                columns=["timestamp", "activity_duration", "activity_intensity", "activity_type",
                                         "steps_boolean"])
        return motionDf

    def readRestHrIos(self, inputBlob):
        restHrData = json.load(inputBlob)
        checks = ['restingHeartRate', 'currentDayRestingHeartRate']
        tsList = [i['timestamp'] for i in restHrData if all([x in i for x in checks])]
        restHrList = [i['restingHeartRate'] for i in restHrData if all([x in i for x in checks])]
        dailyRestHrList = [i['currentDayRestingHeartRate'] for i in restHrData if all([x in i for x in checks])]
        restHrDf = pd.DataFrame(list(zip(tsList, restHrList, dailyRestHrList)),
                                columns=["timestamp", "resting_HR", "daily_resting_HR"])
        return restHrDf

    def readWellnessIos(self, inputBlob):
        wellnessData = json.load(inputBlob)
        checks = ['heartRate', 'steps', 'distance', 'ascent', 'descent', 'moderateActivityMinutes',
                  'vigorousActivityMinutes', 'intensity', 'totalCalories', 'activeCalories']
        tsList = [i['startTime'] for i in wellnessData if all([x in i for x in checks])]
        durationList = [i['elapsedTime'] for i in wellnessData if all([x in i for x in checks])]
        hrList = [i['heartRate'] for i in wellnessData if all([x in i for x in checks])]
        stepsList = [i['steps'] for i in wellnessData if all([x in i for x in checks])]
        distanceList = [i['distance'] for i in wellnessData if all([x in i for x in checks])]
        ascentList = [i['ascent'] for i in wellnessData if all([x in i for x in checks])]
        descentList = [i['descent'] for i in wellnessData if all([x in i for x in checks])]
        descriptionList = [i['description'] for i in wellnessData if all([x in i for x in checks])]
        activityListRaw = [i.split("activityType: ")[-1] for i in descriptionList]
        activityListRaw = [i.split(" ")[0] for i in activityListRaw]
        if any(["\n" in i for i in activityListRaw]):
            activityList = [i.replace("\n", "") for i in activityListRaw if "\n" in i]
        else:
            activityList = activityListRaw
        # print(activityList)
        moderateActivityMinutesList = [i['moderateActivityMinutes'] for i in wellnessData if
                                       all([x in i for x in checks])]
        vigorousActivityMinutesList = [i['vigorousActivityMinutes'] for i in wellnessData if
                                       all([x in i for x in checks])]
        intensityList = [i['intensity'] for i in wellnessData if all([x in i for x in checks])]
        totalCaloriesList = [i['totalCalories'] for i in wellnessData if all([x in i for x in checks])]
        activeCaloriesList = [i['activeCalories'] for i in wellnessData if all([x in i for x in checks])]
        wellnessDf = pd.DataFrame(
            list(zip(tsList, durationList, hrList, stepsList, distanceList, ascentList, descentList,
                     activityList, moderateActivityMinutesList, vigorousActivityMinutesList,
                     intensityList, totalCaloriesList, activeCaloriesList)),
            columns=["timestamp", "window_size", "heart_rate", "step_count", "distance_m", "ascent_m",
                     "descent_m", "activity", "moderate_activity_mins", "vigorous_activity_mins",
                     "intensity", "total_cals_burned", "active_cals_burned"])

        return wellnessDf

    def readSleepIos(self, inputBlob):
        sleepData = json.load(inputBlob)
        checks = ['sleepDurationScore', 'remSleepScore', 'awakeningsCount', 'combinedAwakeScore',
                  'sleepRestlessnessScore',
                  'interruptionsScore', 'sleepQualityScore', 'awakeningsCountScore', 'overallSleepScore',
                  'awakeTimeScore',
                  'deepSleepScore', 'lightSleepScore', 'sleepRecoveryScore']
        toSleepList = [i['startTimestamp'] for i in sleepData if all([x in i for x in checks])]
        getUpList = [i['endTimestamp'] for i in sleepData if all([x in i for x in checks])]
        sleepDurationScoreList = [i['sleepDurationScore'] for i in sleepData if all([x in i for x in checks])]
        remSleepScoreList = [i['remSleepScore'] for i in sleepData if all([x in i for x in checks])]
        awakeningsCountList = [i['awakeningsCount'] for i in sleepData if all([x in i for x in checks])]
        combinedAwakeScoreList = [i['combinedAwakeScore'] for i in sleepData if all([x in i for x in checks])]
        sleepRestlessnessScoreList = [i['sleepRestlessnessScore'] for i in sleepData if all([x in i for x in checks])]
        interruptionsScoreList = [i['interruptionsScore'] for i in sleepData if all([x in i for x in checks])]
        sleepQualityScoreList = [i['sleepQualityScore'] for i in sleepData if all([x in i for x in checks])]
        awakeningsCountScoreList = [i['awakeningsCountScore'] for i in sleepData if all([x in i for x in checks])]
        overallSleepScoreList = [i['overallSleepScore'] for i in sleepData if all([x in i for x in checks])]
        awakeTimeScoreList = [i['awakeTimeScore'] for i in sleepData if all([x in i for x in checks])]
        deepSleepScoreList = [i['deepSleepScore'] for i in sleepData if all([x in i for x in checks])]
        lightSleepScoreList = [i['lightSleepScore'] for i in sleepData if all([x in i for x in checks])]
        sleepRecoveryScoreList = [i['sleepRecoveryScore'] for i in sleepData if all([x in i for x in checks])]
        sleepDf = pd.DataFrame(list(zip(toSleepList, getUpList, sleepDurationScoreList, remSleepScoreList,
                                        awakeningsCountList, combinedAwakeScoreList, sleepRestlessnessScoreList,
                                        interruptionsScoreList, sleepQualityScoreList, awakeningsCountScoreList,
                                        overallSleepScoreList, awakeTimeScoreList, deepSleepScoreList,
                                        lightSleepScoreList, sleepRecoveryScoreList, getUpList)),
                               columns=["to_bed_time", "get_up_time", "sleep_duration_score", "rem_sleep_score",
                                        "awakenings_count", "combined_awake_score", 'sleep_restlessness_score',
                                        "interruptions_score", "sleep_quality_score", "awakenings_count_score",
                                        "overall_sleep_score", "awake_time_score", "deep_sleep_score",
                                        "light_sleep_score", "sleep_recovery_score", "timestamp"])
        return sleepDf

    def readAllJsonIos(self, blobServiceClient, containerSubject, blobNamesList):
        bodyBatteryBlobNamesList = [i for i in blobNamesList if "bodyBattery" in i]
        bb_df = []
        for bbBlobName in bodyBatteryBlobNamesList:
            blobClient = blobServiceClient.get_blob_client(container=containerSubject, blob=bbBlobName)
            with BytesIO() as inputBlob:
                # azure.downloadToStream(blobClient, inputBlob)
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                bb_datafragment = self.readBodyBatteryIos(inputBlob)
                bb_df.append(bb_datafragment)
        if len(bb_df) > 0:
            bb_df = pd.concat(bb_df, axis=0).reset_index(drop=True)
            bb_df = bb_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            bb_df = pd.DataFrame(columns=['timestamp'])

        motionBlobNamesList = [i for i in blobNamesList if "motion" in i]
        motion_df = []
        for motionBlobName in motionBlobNamesList:
            blobClient = blobServiceClient.get_blob_client(container=containerSubject, blob=motionBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                motion_datafragment = self.readMotionIos(inputBlob)
                motion_df.append(motion_datafragment)
        if len(motion_df) > 0:
            motion_df = pd.concat(motion_df, axis=0).reset_index(drop=True)
            motion_df = motion_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            motion_df = pd.DataFrame(columns=['timestamp'])

        restHrBlobNamesList = [i for i in blobNamesList if "restingHeartRate" in i]
        restHr_df = []
        for restHrBlobName in restHrBlobNamesList:
            blobClient = blobServiceClient.get_blob_client(container=containerSubject, blob=restHrBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                restHr_datafragment = self.readRestHrIos(inputBlob)
                restHr_df.append(restHr_datafragment)
        if len(restHr_df) > 0:
            restHr_df = pd.concat(restHr_df, axis=0).reset_index(drop=True)
            restHr_df = restHr_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            restHr_df = pd.DataFrame(columns=['timestamp'])

        wellnessBlobNamesList = [i for i in blobNamesList if "welness" in i]
        wellness_df = []
        for wellnessBlobName in wellnessBlobNamesList:
            blobClient = blobServiceClient.get_blob_client(container=containerSubject, blob=wellnessBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                wellness_datafragment = self.readWellnessIos(inputBlob)
                wellness_df.append(wellness_datafragment)
        if len(wellness_df) > 0:
            wellness_df = pd.concat(wellness_df, axis=0).reset_index(drop=True)
            wellness_df = wellness_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            wellness_df = pd.DataFrame(columns=['timestamp'])

        sleepBlobNamesList = [i for i in blobNamesList if "sleep" in i]
        sleep_df = []
        for sleepBlobName in sleepBlobNamesList:
            blobClient = blobServiceClient.get_blob_client(container=containerSubject, blob=sleepBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                sleep_datafragment = self.readSleepIos(inputBlob)
                sleep_df.append(sleep_datafragment)
        if len(sleep_df) > 0:
            sleep_df = pd.concat(sleep_df, axis=0).reset_index(drop=True)
            sleep_df = sleep_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            sleep_df = pd.DataFrame(columns=['timestamp'])

        if len(restHr_df['timestamp']) > 0:
            all_df = pd.merge(wellness_df, restHr_df, on="timestamp", how="outer")
        else:
            all_df = wellness_df
        if len(motion_df['timestamp']) > 0:
            all_df = pd.merge(all_df, motion_df, on="timestamp", how="outer")
        if len(bb_df['timestamp']) > 0:
            all_df = pd.merge(all_df, bb_df, on="timestamp", how="outer")
        if len(sleep_df['timestamp']) > 0:
            all_df = pd.merge(all_df, sleep_df, on="timestamp", how="outer")
        all_df = all_df.sort_values("timestamp").reset_index(drop=True)
        all_df.insert(1, "local_time",
                      [dt.fromtimestamp(i, pytz.utc).astimezone(pytz.timezone('Europe/Amsterdam')).strftime(
                          '%Y-%m-%d %H:%M:%S %Z%z')
                          for i in all_df['timestamp']])
        return all_df

    def readHrAndroid(self, inputBlob):
        heartrateData = json.load(inputBlob)
        heartrateList = [i['heartRate'] for i in heartrateData if 'heartRate' in i]
        statusList = [i['status'] for i in heartrateData if 'heartRate' in i]
        localTimeList = [dt(i['timestamp']['date']['year'],
                            i['timestamp']['date']['month'],
                            i['timestamp']['date']['day'],
                            i['timestamp']['time']['hour'],
                            i['timestamp']['time']['minute'],
                            i['timestamp']['time']['second'],
                            tzinfo=pytz.utc) for i in heartrateData if 'heartRate' in i]
        localTimeList = [i.astimezone(pytz.timezone('Europe/Amsterdam'))
                         for i in localTimeList]
        heartrateDf = pd.DataFrame(list(zip(localTimeList, statusList, heartrateList)),
                                   columns=["local_time", "heart_rate_status", "heart_rate"])
        return heartrateDf

    def readHrvAndroid(self, inputBlob):
        hrvData = json.load(inputBlob)
        bbiList = [i['bbi'] for i in hrvData if 'bbi' in i]
        localTimeList = [dt(i['timestamp']['date']['year'],
                            i['timestamp']['date']['month'],
                            i['timestamp']['date']['day'],
                            i['timestamp']['time']['hour'],
                            i['timestamp']['time']['minute'],
                            i['timestamp']['time']['second'],
                            tzinfo=pytz.utc) for i in hrvData if 'bbi' in i]
        localTimeList = [i.astimezone(pytz.timezone('Europe/Amsterdam'))
                         for i in localTimeList]
        hrvDf = pd.DataFrame(list(zip(localTimeList, bbiList)),
                             columns=["local_time", "hrv_bbi"])
        return hrvDf

    def readRespirationAndroid(self, inputBlob):
        respirationData = json.load(inputBlob)
        breathsPerMinList = [i['breathsPerMinute'] for i in respirationData if 'breathsPerMinute' in i]
        localTimeList = [dt(i['timestamp']['date']['year'],
                            i['timestamp']['date']['month'],
                            i['timestamp']['date']['day'],
                            i['timestamp']['time']['hour'],
                            i['timestamp']['time']['minute'],
                            i['timestamp']['time']['second'],
                            tzinfo=pytz.utc) for i in respirationData if 'breathsPerMinute' in i]
        localTimeList = [i.astimezone(pytz.timezone('Europe/Amsterdam'))
                         for i in localTimeList]
        respirationDf = pd.DataFrame(list(zip(localTimeList, breathsPerMinList)),
                                     columns=["local_time", "breathing_rate"])
        return respirationDf

    def readStepsAndroid(self, inputBlob):
        stepsData = json.load(inputBlob)
        stepCountList = [i['stepCount'] for i in stepsData if 'stepCount' in i]
        totalStepsList = [i['totalSteps'] for i in stepsData if 'stepCount' in i]
        localTimeList = [dt(i['startTimestamp']['date']['year'],
                            i['startTimestamp']['date']['month'],
                            i['startTimestamp']['date']['day'],
                            i['startTimestamp']['time']['hour'],
                            i['startTimestamp']['time']['minute'],
                            i['startTimestamp']['time']['second'],
                            tzinfo=pytz.utc) for i in stepsData if 'stepCount' in i]
        localTimeList = [i.astimezone(pytz.timezone('Europe/Amsterdam'))
                         for i in localTimeList]
        endTimeList = [dt(i['endTimestamp']['date']['year'],
                          i['endTimestamp']['date']['month'],
                          i['endTimestamp']['date']['day'],
                          i['endTimestamp']['time']['hour'],
                          i['endTimestamp']['time']['minute'],
                          i['endTimestamp']['time']['second'],
                          tzinfo=pytz.utc) for i in stepsData if 'stepCount' in i]
        endTimeList = [i.astimezone(pytz.timezone('Europe/Amsterdam'))
                       for i in endTimeList]
        durationList = [dt.timestamp(tEnd) - dt.timestamp(tStart) for tEnd, tStart in zip(endTimeList, localTimeList)]
        respirationDf = pd.DataFrame(list(zip(localTimeList, durationList, stepCountList, totalStepsList)),
                                     columns=["local_time", "steps_window_size", "step_count", "total_steps"])
        return respirationDf

    def readStressAndroid(self, inputBlob):
        stressData = json.load(inputBlob)
        stressScoreList = [i['stressScore'] for i in stressData if 'stressScore' in i]
        stressStatusList = [i['stressStatus'] for i in stressData if 'stressScore' in i]
        localTimeList = [dt(i['timestamp']['date']['year'],
                            i['timestamp']['date']['month'],
                            i['timestamp']['date']['day'],
                            i['timestamp']['time']['hour'],
                            i['timestamp']['time']['minute'],
                            i['timestamp']['time']['second'],
                            tzinfo=pytz.utc) for i in stressData if 'stressScore' in i]
        localTimeList = [i.astimezone(pytz.timezone('Europe/Amsterdam'))
                         for i in localTimeList]
        stressDf = pd.DataFrame(list(zip(localTimeList, stressStatusList, stressScoreList)),
                                columns=["local_time", "stress_status", "stress_score"])
        return stressDf

    def readZeroCrossingAndroid(self, inputBlob):
        zerocrossingData = json.load(inputBlob)
        zeroCrossingCountList = [i['zeroCrossingCount'] for i in zerocrossingData if 'zeroCrossingCount' in i]
        totalEnergyList = [i['totalEnergy'] for i in zerocrossingData if 'zeroCrossingCount' in i]
        localTimeList = [dt(i['startTimestamp']['date']['year'],
                            i['startTimestamp']['date']['month'],
                            i['startTimestamp']['date']['day'],
                            i['startTimestamp']['time']['hour'],
                            i['startTimestamp']['time']['minute'],
                            i['startTimestamp']['time']['second'],
                            tzinfo=pytz.utc) for i in zerocrossingData if 'zeroCrossingCount' in i]
        localTimeList = [i.astimezone(pytz.timezone('Europe/Amsterdam'))
                         for i in localTimeList]
        endTimeList = [dt(i['endTimestamp']['date']['year'],
                          i['endTimestamp']['date']['month'],
                          i['endTimestamp']['date']['day'],
                          i['endTimestamp']['time']['hour'],
                          i['endTimestamp']['time']['minute'],
                          i['endTimestamp']['time']['second'],
                          tzinfo=pytz.utc) for i in zerocrossingData if 'zeroCrossingCount' in i]
        endTimeList = [i.astimezone(pytz.timezone('Europe/Amsterdam'))
                       for i in endTimeList]
        durationList = [dt.timestamp(tEnd) - dt.timestamp(tStart) for tEnd, tStart in zip(endTimeList, localTimeList)]
        zerocrossingDf = pd.DataFrame(list(zip(localTimeList, durationList, zeroCrossingCountList, totalEnergyList)),
                                      columns=["local_time", "zero_crossing_window_size",
                                               "zero_crossing_count", "total_energy"])
        return zerocrossingDf

    def readAllJsonAndroid(self, blobServiceClient, containerSubject, blobNamesList, summarizePerMinute):
        heartrateBlobNamesList = [i for i in blobNamesList if "heartrate" in i]
        heartrate_df = []
        for heartrateBlobName in heartrateBlobNamesList:
            blobClient = self.blob_service_client.get_blob_client(container=containerSubject, blob=heartrateBlobName)
            with BytesIO() as inputBlob:
                # azure.downloadToStream(blobClient, inputBlob)
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                heartrate_datafragment = self.readHrAndroid(inputBlob)
                heartrate_df.append(heartrate_datafragment)
        if len(heartrate_df) > 0:
            heartrate_df = pd.concat(heartrate_df, axis=0).reset_index(drop=True)
            heartrate_df = heartrate_df.drop_duplicates(subset='local_time', keep='first')
        else:
            heartrate_df = pd.DataFrame(columns=['timestamp'])

        hrvBlobNamesList = [i for i in blobNamesList if "hrv" in i]
        hrv_df = []
        for hrvBlobName in hrvBlobNamesList:
            blobClient = self.blob_service_client.get_blob_client(container=containerSubject, blob=hrvBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                hrv_datafragment = self.readHrvAndroid(inputBlob)
                hrv_df.append(hrv_datafragment)
        if len(hrv_df) > 0:
            hrv_df = pd.concat(hrv_df, axis=0).reset_index(drop=True)
            hrv_df = hrv_df.drop_duplicates(subset='local_time', keep='first')
        else:
            hrv_df = pd.DataFrame(columns=['timestamp'])

        respirationBlobNamesList = [i for i in blobNamesList if "respiration" in i]
        respiration_df = []
        for respirationBlobName in respirationBlobNamesList:
            blobClient = self.blob_service_client.get_blob_client(container=containerSubject, blob=respirationBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                respiration_datafragment = self.readRespirationAndroid(inputBlob)
                respiration_df.append(respiration_datafragment)
        if len(respiration_df) > 0:
            respiration_df = pd.concat(respiration_df, axis=0).reset_index(drop=True)
            respiration_df = respiration_df.drop_duplicates(subset='local_time', keep='first')
        else:
            respiration_df = pd.DataFrame(columns=['timestamp'])

        stepsBlobNamesList = [i for i in blobNamesList if "steps" in i]
        steps_df = []
        for stepsBlobName in stepsBlobNamesList:
            blobClient = self.blob_service_client.get_blob_client(container=containerSubject, blob=stepsBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                steps_datafragment = self.readStepsAndroid(inputBlob)
                steps_df.append(steps_datafragment)
        if len(steps_df) > 0:
            steps_df = pd.concat(steps_df, axis=0).reset_index(drop=True)
            steps_df = steps_df.drop_duplicates(subset='local_time', keep='first')
        else:
            steps_df = pd.DataFrame(columns=['timestamp'])

        stressBlobNamesList = [i for i in blobNamesList if "stress" in i]
        stress_df = []
        for stressBlobName in stressBlobNamesList:
            blobClient = self.blob_service_client.get_blob_client(container=containerSubject, blob=stressBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                stress_datafragment = self.readStressAndroid(inputBlob)
                stress_df.append(stress_datafragment)
        if len(stress_df) > 0:
            stress_df = pd.concat(stress_df, axis=0).reset_index(drop=True)
            stress_df = stress_df.drop_duplicates(subset='local_time', keep='first')
        else:
            stress_df = pd.DataFrame(columns=['timestamp'])

        zerocrossingBlobNamesList = [i for i in blobNamesList if "zerocrossing" in i]
        zerocrossing_df = []
        for zerocrossingBlobName in zerocrossingBlobNamesList:
            blobClient = self.blob_service_client.get_blob_client(container=containerSubject, blob=zerocrossingBlobName)
            with BytesIO() as inputBlob:
                blobClient.download_blob().download_to_stream(inputBlob)
                inputBlob.seek(0)
                zerocrossing_datafragment = self.readZeroCrossingAndroid(inputBlob)
                zerocrossing_df.append(zerocrossing_datafragment)
        if len(zerocrossing_df) > 0:
            zerocrossing_df = pd.concat(zerocrossing_df, axis=0).reset_index(drop=True)
            zerocrossing_df = zerocrossing_df.drop_duplicates(subset='local_time', keep='first')
        else:
            zerocrossing_df = pd.DataFrame(columns=['timestamp'])

        all_df = pd.merge(heartrate_df, hrv_df, on="local_time", how="outer")
        all_df = pd.merge(all_df, respiration_df, on="local_time", how="outer")
        all_df = pd.merge(all_df, steps_df, on="local_time", how="outer")
        all_df = pd.merge(all_df, stress_df, on="local_time", how="outer")
        all_df = pd.merge(all_df, zerocrossing_df, on="local_time", how="outer")
        all_df.insert(0, "timestamp", [dt.timestamp(i) for i in all_df['local_time']])
        all_df = all_df.sort_values("timestamp").reset_index(drop=True)
        if summarizePerMinute:
            all_df.insert(0, "minutes_unix", [math.floor(i / 60) for i in all_df['timestamp']])
            all_df = all_df.groupby("minutes_unix").agg(timestamp=('timestamp', 'min'),
                                                        local_time=('local_time', 'min'),
                                                        heart_rate_status=('heart_rate_status', set),
                                                        heart_rate=('heart_rate', 'mean'),
                                                        heart_rate_min=('heart_rate', 'min'),
                                                        heart_rate_max=('heart_rate', 'max'),
                                                        hrv_bbi_mean=('hrv_bbi', 'mean'),
                                                        hrv_bbi_std=('hrv_bbi', np.std),
                                                        hrv_bbi_min=('hrv_bbi', 'min'),
                                                        hrv_bbi_max=('hrv_bbi', 'max'),
                                                        breathing_rate_mean=('breathing_rate', 'mean'),
                                                        breathing_rate_min=('breathing_rate', 'min'),
                                                        breathing_rate_max=('breathing_rate', 'max'),
                                                        step_count=('step_count', 'sum'),
                                                        total_steps=('total_steps', 'max'),
                                                        stress_status=('stress_status', set),
                                                        stress_score_mean=('stress_score', 'mean'),
                                                        stress_score_min=('stress_score', 'min'),
                                                        stress_score_max=('stress_score', 'max'),
                                                        zero_crossing_count=('zero_crossing_count', 'sum'),
                                                        total_energy=('total_energy', 'max'))
            all_df = all_df.reset_index(drop=True)

        return all_df

    def getGarminDataSubject(self, subjectID, summarizePerMinute, container="participants"):  # , lastUpdatedItem
        containerSubject = "participants"
        container_client = self.blob_service_client.get_container_client(container=container)
        start_string = subjectID + "/garmindata/sync_"
        blob_names_list = list(container_client.list_blob_names(name_starts_with=start_string))
        print(blob_names_list)
        # Iterate over each file name in blob_names_list
        for blob_name in blob_names_list:
            # Check if the file name ends with 'loggedStressData.json'
            if blob_name.endswith('loggedStressData.json'):
                print(blob_name)
        if len(blob_names_list) == 0:
            raise FileNotFoundError(
                "The data of this subject has not been unzipped yet. Please first run unzipSubject.")
        operating_system = self.getOperatingSystem(blob_names_list[0])
        if operating_system == "ios":
            garmin_data = self.readAllJsonIos(self.blob_service_client, containerSubject, blob_names_list)
        elif operating_system == "android":
            garmin_data = self.readAllJsonAndroid(self.blob_service_client, containerSubject, blob_names_list, summarizePerMinute)
        else:
            raise OSError("The operating system is unknown. The options are android or ios.")
        garmin_data.insert(0, "subject", [subjectID] * len(garmin_data['timestamp']))
        garmin_data.insert(1, "os_type", [operating_system] * len(garmin_data['timestamp']))
        return garmin_data

    def getGarminDataGroup(self, subjectList, summarizePerMinute):
        garminData = []
        for subject in subjectList:
            subjectData = self.getGarminDataSubject(subject, summarizePerMinute)
            garminData.append(subjectData)
        garminData = pd.concat(garminData, axis=0)
        garminData = garminData.reset_index(drop=True)
        return garminData

    def getUniqueFileNames(self, subjectID, container="participants"):
        container_client = self.blob_service_client.get_container_client(container=container)
        start_string = subjectID + "/garmindata/sync"

        blob_names_list = list(container_client.list_blob_names(name_starts_with=start_string))
        if len(blob_names_list) == 0:
            raise FileNotFoundError(
                "The data of this subject has not been unzipped yet. Please first run unzipSubject.")
        fileList = [i.split("_")[-1] for i in blob_names_list]
        uniqueFileList = list(set(fileList))
        return uniqueFileList


if __name__ == "__main__":
    # The script is based on generating a SAS token to use for data access.
    # first "get shared access signature", then select the options you desire for permissions and other parameters and click "create".
    # In the connection string field, you will find a part that says "BlobEndpoint=https....". Everything from the "https:" till ".net/" is put below as account_url.
    # For filling in the sas_token variable below you should use the copy button below the "SAS token" field.
    # Check if your account_url indeed starts with "https://" and ends with ".net/"
    # Check if your sas_token starts with a ?.
    # You are good to go and use this script.
    # STUDY ENVIRONMENT PARAMETERS
    account_url = "https://vitalityhubwearabledata.blob.core.windows.net/"  # See explanation above
    sas_token = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-07-28T17:31:44Z&st=2024-03-28T10:31:44Z&spr=https,http&sig=SR6G0FBa1z28RunlFPhDzv4%2FhEfKcvKd5gXGd8bkoZM%3D"
    subject = "testParticipant01"  # Select a subject that exists (see folders in "participants" container)
    subject_list = ["", "", "", ""]  # Select multiple subjects that exist (see folders in "participants" container)
    # CALL FUNCTIONS
    # For unzipping and preprocessing multiple subjects at once.
    # Garmin.unzipGroup(account_url, sas_token, subject_list)
    # testdata = Garmin.getGarminDataGroup(account_url, sas_token, subject_list, False)
    # testdata.to_csv("garmin_data.csv")

    # For unzipping and preprocessing only one subject.
    garmin = Garmin(account_url, sas_token)
    garmin.unzipSubject(subject)


    # def getGarminDataSubject(accountURL, sasToken, subjectID, summarizePerMinute, blobServiceClient, container):
    testdata = garmin.getGarminDataSubject(subject, summarizePerMinute=False)
    testdata.to_csv("garmin_data.csv")

    # weten dat er nieuwe data is
    # nieuwe data krijgen zonder de oude data

    data = pd.read_csv("garmin_data.csv")

    # Example: Selecting specific columns
    # print(data)
    selected_columns = data[['heart_rate', 'resting_HR', 'timestamp']]

    resting_HR = data[['resting_HR']]

    print("resting_HR: ", resting_HR)
    print(selected_columns)
