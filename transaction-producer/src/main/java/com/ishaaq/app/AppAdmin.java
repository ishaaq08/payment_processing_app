package com.ishaaq.app;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;

public class AppAdmin extends Builder<Admin> {

    public AppAdmin(Map<String, Object> configs) {
        super(configs);
    }

    // Private/helper methods
    @Override
    public void createClient() {
        System.out.println("--> creating admin client with configs: " + configs);
        super.client = Admin.create(configs);
    }
}