package com.epam.naya_bdd_project.poc_app.demo001_multi_module_project;

import com.epam.naya_bdd_project.common.utils.DummyUtil_java;

public class Main_java {
    public static void main(String[] args) {
        System.out.println("poc main (java) sais hi");
        System.out.println("poc main (java) calls util in java and got:" + DummyUtil_java.sayHi());

    }
}
