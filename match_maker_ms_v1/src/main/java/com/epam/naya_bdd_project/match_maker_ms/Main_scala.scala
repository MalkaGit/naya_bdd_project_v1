package com.epam.naya_bdd_project.match_maker_ms
import com.epam.naya_bdd_project.common.utils.DummyUtil_scala


object Main_scala {
  def main(args: Array[String]): Unit = {
    System.out.println("poc main (java) sais hi")

    System.out.println("poc main (java) calls util in java and got:" + DummyUtil_scala.sayHi)
  }

}
