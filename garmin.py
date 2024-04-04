from azure_handling import AzureHandler as azure

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
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

class Garmin:

    def __init__(self):
        self.a = []

    def unzipGarminFile(inputBlob, tempLocalPath):
        with ZipFile(inputBlob, 'r') as zipObj:
            zipObj.extractall(tempLocalPath)

    def unzipSubject(accountURL, sasToken, subjectID, containerSubject="participants"):
        # Basically loggin in into Azure
        blob_service_client = azure.getBlobServiceClient(accountURL, sasToken)
        container_client = azure.getContainer(blob_service_client, containerSubject) # This gets all the blob names from Azure
        start_string = subjectID + "/sensordata/sync"
        blob_names_list = azure.listBlobs(container_client, start_string)
        print ("these are the blob name lists: ", blob_names_list)
        print("This is the first blob in blob name lists: ", blob_names_list[-1])
        # This lists all the blobs (zip files) in participants/sensorsdata/sync*....*.zip
        # last_updated_item_blob = sorted(blob_names_list, key=lambda x: x[1])[-1] #sorteert de lijst blobnames list op de eerste element van elk tuple en neemt hierna de eerste in de array
        # print("last updated= ", last_updated_item_blob)
        temporary_local_path = os.getcwd() + "/" + subjectID
        os.makedirs(temporary_local_path)
        for blob_name in blob_names_list:
            blob_client = azure.getBlob(blob_service_client, containerSubject, blob_name) # This is sort of a reference to the blob data on Azure
            with BytesIO() as input_blob:            # Writes data to a temporary file
                azure.downloadToStream(blob_client, input_blob)
                input_blob.seek(0) # This can, I think, be removed...
                Garmin.unzipGarminFile(input_blob, temporary_local_path)
            datafolder = os.listdir(temporary_local_path)[0]
            folder_path = temporary_local_path + "/" + datafolder
            datafile_list = os.listdir(folder_path)
            for datafile in datafile_list:
                file_path = folder_path + "/" + datafile
                datafile_name = "/" + datafile
                new_blob_name = blob_name.replace("sensordata", "garmindata")
                new_blob_name = new_blob_name.replace(".zip", datafile_name)
                azure.uploadBlobFile(container_client, file_path, new_blob_name)
            shutil.rmtree(folder_path)
        shutil.rmtree(temporary_local_path)

    def unzipGroup(accountURL, sasToken, subjectList):
        for subject in subjectList:
            Garmin.unzipSubject(accountURL, sasToken, subject)

    def getOperatingSystem(blobName):
        filename = blobName.split('/')[-1]
        if filename[0].isnumeric():
            return "android"
        elif not filename[0].isnumeric():
            return "ios"
        else:
            return "unknown_os"

    def readBodyBatteryIos(inputBlob):
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

    def readMotionIos(inputBlob):
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

    def readRestHrIos(inputBlob):
        restHrData = json.load(inputBlob)
        checks = ['restingHeartRate', 'currentDayRestingHeartRate']
        tsList = [i['timestamp'] for i in restHrData if all([x in i for x in checks])]
        restHrList = [i['restingHeartRate'] for i in restHrData if all([x in i for x in checks])]
        dailyRestHrList = [i['currentDayRestingHeartRate'] for i in restHrData if all([x in i for x in checks])]
        restHrDf = pd.DataFrame(list(zip(tsList, restHrList, dailyRestHrList)),
                                columns=["timestamp", "resting_HR", "daily_resting_HR"])
        return restHrDf

    def readWellnessIos(inputBlob):
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

    def readSleepIos(inputBlob):
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

    def readAllJsonIos(blobServiceClient, containerSubject, blobNamesList):
        bodyBatteryBlobNamesList = [i for i in blobNamesList if "bodyBattery" in i]
        bb_df = []
        for bbBlobName in bodyBatteryBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, bbBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                bb_datafragment = Garmin.readBodyBatteryIos(inputBlob)
                bb_df.append(bb_datafragment)
        if len(bb_df) > 0:
            bb_df = pd.concat(bb_df, axis=0).reset_index(drop=True)
            bb_df = bb_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            bb_df = pd.DataFrame(columns=['timestamp'])

        motionBlobNamesList = [i for i in blobNamesList if "motion" in i]
        motion_df = []
        for motionBlobName in motionBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, motionBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                motion_datafragment = Garmin.readMotionIos(inputBlob)
                motion_df.append(motion_datafragment)
        if len(motion_df) > 0:
            motion_df = pd.concat(motion_df, axis=0).reset_index(drop=True)
            motion_df = motion_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            motion_df = pd.DataFrame(columns=['timestamp'])

        restHrBlobNamesList = [i for i in blobNamesList if "restingHeartRate" in i]
        restHr_df = []
        for restHrBlobName in restHrBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, restHrBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                restHr_datafragment = Garmin.readRestHrIos(inputBlob)
                restHr_df.append(restHr_datafragment)
        if len(restHr_df) > 0:
            restHr_df = pd.concat(restHr_df, axis=0).reset_index(drop=True)
            restHr_df = restHr_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            restHr_df = pd.DataFrame(columns=['timestamp'])

        wellnessBlobNamesList = [i for i in blobNamesList if "welness" in i]
        wellness_df = []
        for wellnessBlobName in wellnessBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, wellnessBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                wellness_datafragment = Garmin.readWellnessIos(inputBlob)
                wellness_df.append(wellness_datafragment)
        if len(wellness_df) > 0:
            wellness_df = pd.concat(wellness_df, axis=0).reset_index(drop=True)
            wellness_df = wellness_df.drop_duplicates(subset='timestamp', keep='first')
        else:
            wellness_df = pd.DataFrame(columns=['timestamp'])

        sleepBlobNamesList = [i for i in blobNamesList if "sleep" in i]
        sleep_df = []
        for sleepBlobName in sleepBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, sleepBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                sleep_datafragment = Garmin.readSleepIos(inputBlob)
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

    def readHrAndroid(inputBlob):
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

    def readHrvAndroid(inputBlob):
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

    def readRespirationAndroid(inputBlob):
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

    def readStepsAndroid(inputBlob):
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

    def readStressAndroid(inputBlob):
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

    def readZeroCrossingAndroid(inputBlob):
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

    def readAllJsonAndroid(blobServiceClient, containerSubject, blobNamesList, summarizePerMinute):
        heartrateBlobNamesList = [i for i in blobNamesList if "heartrate" in i]
        heartrate_df = []
        for heartrateBlobName in heartrateBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, heartrateBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                heartrate_datafragment = Garmin.readHrAndroid(inputBlob)
                heartrate_df.append(heartrate_datafragment)
        if len(heartrate_df) > 0:
            heartrate_df = pd.concat(heartrate_df, axis=0).reset_index(drop=True)
            heartrate_df = heartrate_df.drop_duplicates(subset='local_time', keep='first')
        else:
            heartrate_df = pd.DataFrame(columns=['timestamp'])

        hrvBlobNamesList = [i for i in blobNamesList if "hrv" in i]
        hrv_df = []
        for hrvBlobName in hrvBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, hrvBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                hrv_datafragment = Garmin.readHrvAndroid(inputBlob)
                hrv_df.append(hrv_datafragment)
        if len(hrv_df) > 0:
            hrv_df = pd.concat(hrv_df, axis=0).reset_index(drop=True)
            hrv_df = hrv_df.drop_duplicates(subset='local_time', keep='first')
        else:
            hrv_df = pd.DataFrame(columns=['timestamp'])

        respirationBlobNamesList = [i for i in blobNamesList if "respiration" in i]
        respiration_df = []
        for respirationBlobName in respirationBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, respirationBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                respiration_datafragment = Garmin.readRespirationAndroid(inputBlob)
                respiration_df.append(respiration_datafragment)
        if len(respiration_df) > 0:
            respiration_df = pd.concat(respiration_df, axis=0).reset_index(drop=True)
            respiration_df = respiration_df.drop_duplicates(subset='local_time', keep='first')
        else:
            respiration_df = pd.DataFrame(columns=['timestamp'])

        stepsBlobNamesList = [i for i in blobNamesList if "steps" in i]
        steps_df = []
        for stepsBlobName in stepsBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, stepsBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                steps_datafragment = Garmin.readStepsAndroid(inputBlob)
                steps_df.append(steps_datafragment)
        if len(steps_df) > 0:
            steps_df = pd.concat(steps_df, axis=0).reset_index(drop=True)
            steps_df = steps_df.drop_duplicates(subset='local_time', keep='first')
        else:
            steps_df = pd.DataFrame(columns=['timestamp'])

        stressBlobNamesList = [i for i in blobNamesList if "stress" in i]
        stress_df = []
        for stressBlobName in stressBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, stressBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                stress_datafragment = Garmin.readStressAndroid(inputBlob)
                stress_df.append(stress_datafragment)
        if len(stress_df) > 0:
            stress_df = pd.concat(stress_df, axis=0).reset_index(drop=True)
            stress_df = stress_df.drop_duplicates(subset='local_time', keep='first')
        else:
            stress_df = pd.DataFrame(columns=['timestamp'])

        zerocrossingBlobNamesList = [i for i in blobNamesList if "zerocrossing" in i]
        zerocrossing_df = []
        for zerocrossingBlobName in zerocrossingBlobNamesList:
            blobClient = azure.getBlob(blobServiceClient, containerSubject, zerocrossingBlobName)
            with BytesIO() as inputBlob:
                azure.downloadToStream(blobClient, inputBlob)
                inputBlob.seek(0)
                zerocrossing_datafragment = Garmin.readZeroCrossingAndroid(inputBlob)
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

    def getGarminDataSubject(accountURL, sasToken, subjectID, summarizePerMinute):#, lastUpdatedItem
        blob_service_client = azure.getBlobServiceClient(accountURL, sasToken)
        containerSubject = "participants"
        container_client = azure.getContainer(blob_service_client, containerSubject)
        start_string = subjectID + "/garmindata/sync_"
        blob_names_list = azure.listBlobs(container_client, start_string)
        print(blob_names_list)
        # Iterate over each file name in blob_names_list
        for blob_name in blob_names_list:
            # Check if the file name ends with 'loggedStressData.json'
            if blob_name.endswith('loggedStressData.json'):
                print(blob_name)
        if len(blob_names_list) == 0:
            raise FileNotFoundError(
                "The data of this subject has not been unzipped yet. Please first run unzipSubject.")
        operating_system = Garmin.getOperatingSystem(blob_names_list[0])
        if operating_system == "ios":
            garmin_data = Garmin.readAllJsonIos(blob_service_client, containerSubject, blob_names_list)
        elif operating_system == "android":
            garmin_data = Garmin.readAllJsonAndroid(blob_service_client, containerSubject, blob_names_list,
                                                    summarizePerMinute)
        else:
            raise OSError("The operating system is unknown. The options are android or ios.")
        garmin_data.insert(0, "subject", [subjectID] * len(garmin_data['timestamp']))
        garmin_data.insert(1, "os_type", [operating_system] * len(garmin_data['timestamp']))
        return garmin_data

    def getGarminDataGroup(accountURL, sasToken, subjectList, summarizePerMinute):
        garminData = []
        for subject in subjectList:
            subjectData = Garmin.getGarminDataSubject(accountURL, sasToken, subject, summarizePerMinute)
            garminData.append(subjectData)
        garminData = pd.concat(garminData, axis=0)
        garminData = garminData.reset_index(drop=True)
        return garminData

    def getUniqueFileNames(accountURL, sasToken, subjectID):
        blob_service_client = azure.getBlobServiceClient(accountURL, sasToken)
        containerSubject = "participants"
        container_client = azure.getContainer(blob_service_client, containerSubject)
        start_string = subjectID + "/garmindata/sync"
        blob_names_list = azure.listBlobs(container_client, start_string)
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
    sas_token= "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-07-28T17:31:44Z&st=2024-03-28T10:31:44Z&spr=https,http&sig=SR6G0FBa1z28RunlFPhDzv4%2FhEfKcvKd5gXGd8bkoZM%3D"
    subject = "testParticipant01"  # Select a subject that exists (see folders in "participants" container)
    subject_list = ["", "", "", ""]  # Select multiple subjects that exist (see folders in "participants" container)
    # CALL FUNCTIONS
    # For unzipping and preprocessing multiple subjects at once.
    # Garmin.unzipGroup(account_url, sas_token, subject_list)
    # testdata = Garmin.getGarminDataGroup(account_url, sas_token, subject_list, False)
    # testdata.to_csv("garmin_data.csv")

    # For unzipping and preprocessing only one subject.
    Garmin.unzipSubject(account_url, sas_token, subject)
    testdata = Garmin.getGarminDataSubject(account_url, sas_token, subject, summarizePerMinute=False)
    testdata.to_csv("garmin_data.csv")

    #weten dat er nieuwe data is
    #nieuwe data krijgen zonder de oude data


    data = pd.read_csv("garmin_data.csv")

    # Example: Selecting specific columns
    # print(data)
    selected_columns = data[['heart_rate', 'resting_HR', 'timestamp']]

    resting_HR = data[['resting_HR']]

    print("resting_HR: ", resting_HR)
    print(selected_columns)