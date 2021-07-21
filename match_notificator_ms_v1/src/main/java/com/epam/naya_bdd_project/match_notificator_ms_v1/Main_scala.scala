package com.epam.naya_bdd_project.match_notificator_ms_v1

import com.epam.naya_bdd_project.common.utils.DummyUtil_scala
import com.epam.naya_bdd_project.match_notificator_ms_v1.utils.MailSenderUtil

object Main_scala {

  def main(args: Array[String]): Unit = {
    System.out.println("poc main (scala) sais hi !")
    System.out.println("poc main (scal) calls util in java and got:" + DummyUtil_scala.sayHi)

    MailSenderUtil.send("regine.issan.jobs@gmail.com","regine.issan@gmail.com","Pumiki14","s1","b1");
  }

}
