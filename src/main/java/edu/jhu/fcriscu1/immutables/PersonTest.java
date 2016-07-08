package edu.jhu.fcriscu1.immutables;

import lombok.extern.log4j.Log4j;

/**
 * Created by fcriscuolo on 7/8/16.
 */
@Log4j
public class PersonTest {
    public static void main(String[] args) {
        Person person = new PersonBuilder()
                .name("Jim Boe")
                .address("P.O. box 0001, Lexington, KY")
                .build();
        log.info("Person: " +person.getName());

    }
}
