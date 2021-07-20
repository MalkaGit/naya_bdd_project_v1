package com.epam.naya_bdd_project.common.models

case class Match(stream_offer_guid: String, stream_user_id: Int, stream_give: String, stream_take: String,
                 db_offer_guid:String, db_user_id: Int, db_give: String, db_take: String,
                 stream_email: String, stream_fname: String, stream_lname: String,
                 db_email: String, db_fname: String, db_lname: String
                )