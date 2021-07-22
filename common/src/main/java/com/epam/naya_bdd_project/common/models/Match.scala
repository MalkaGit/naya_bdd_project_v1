package com.epam.naya_bdd_project.common.models

case class Match(streamOfferGuid: String, streamUserId: Int, streamGive: String, streamTake: String,
                 dbOfferGuid:String, dbUserId: Int, dbGive: String, dbTake: String,
                 streamEmail: String, streamFname: String, streamLname: String,
                 dbEmail: String, dbFname: String, dbLname: String
                )