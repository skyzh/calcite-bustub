package org.example;

public class Main {
    public static void main(String[] args) {
        BusTubConnector b = new BusTubConnector("http://localhost:23333");
        try {
            b.run();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}