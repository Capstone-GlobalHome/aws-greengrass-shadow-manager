/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class ShadowManagerDAOImplTest {

    private static final String MISSING_THING_NAME = "missingTestThing";
    private static final String CLASSIC_SHADOW_THING = "classicThing";
    private static final byte[] BASE_DOCUMENT = "{\"id\": 1, \"name\": \"The Beatles\"}".getBytes();
    private static final byte[] NO_SHADOW_NAME_BASE_DOCUMENT = "{\"id\": 2, \"name\": \"The Beach Boys\"}".getBytes();
    private static final byte[] UPDATED_DOCUMENT = "{\"id\": 1, \"name\": \"New Name\"}".getBytes();
    private static final List<String> SHADOW_NAME_LIST = Arrays.asList("alpha", "bravo", "charlie", "delta");
    private static final int DEFAULT_OFFSET = 0;
    private static final int DEFAULT_LIMIT = 25;


    @TempDir
    Path rootDir;

    private Kernel kernel;
    private ShadowManagerDatabase database;
    private ShadowManagerDAOImpl dao;

    @Inject
    public ShadowManagerDAOImplTest() {
    }

    @BeforeEach
    public void before() throws SQLException {
        kernel = new Kernel();
        // Might need to start the kernel here
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString());

        database = new ShadowManagerDatabase(kernel);
        database.install();
        dao = new ShadowManagerDAOImpl(database);
    }

    @AfterEach
    void cleanup() throws IOException {
        database.close();
        kernel.shutdown();
    }

    private static Stream<Arguments> classicAndNamedShadow() {
        return Stream.of(
                arguments(SHADOW_NAME, BASE_DOCUMENT),
                arguments(CLASSIC_SHADOW_IDENTIFIER, NO_SHADOW_NAME_BASE_DOCUMENT)
        );
    }

    void createNamedShadow() throws Exception {
        Optional<byte[]> result = dao.createShadowThing(THING_NAME, SHADOW_NAME, BASE_DOCUMENT);
        assertThat("Created named shadow", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(BASE_DOCUMENT)));
    }

    void createClassicShadow() throws Exception {
        Optional<byte[]> result = dao.createShadowThing(THING_NAME, CLASSIC_SHADOW_IDENTIFIER, NO_SHADOW_NAME_BASE_DOCUMENT);
        assertThat("Created classic shadow", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(NO_SHADOW_NAME_BASE_DOCUMENT)));
    }

    @ParameterizedTest
    @MethodSource("classicAndNamedShadow")
    void GIVEN_named_and_classic_shadow_WHEN_get_shadow_thing_THEN_return_correct_payload(String shadowName, byte[] expectedPayload) throws Exception {
        createNamedShadow();
        createClassicShadow();
        Optional<byte[]> result = dao.getShadowThing(THING_NAME, shadowName); // NOPMD
        assertThat("Retrieved shadow", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(expectedPayload)));
    }

    @Test
    void GIVEN_no_shadow_WHEN_get_shadow_thing_THEN_return_nothing() throws Exception {
        Optional<byte[]> result = dao.getShadowThing(MISSING_THING_NAME, SHADOW_NAME);
        assertThat("No shadow found", result.isPresent(), is(false));
    }

    @ParameterizedTest
    @MethodSource("classicAndNamedShadow")
    void GIVEN_named_and_classic_shadow_WHEN_delete_shadow_thing_THEN_shadow_deleted(String shadowName, byte[] expectedPayload) throws Exception {
        createNamedShadow();
        createClassicShadow();
        Optional<byte[]> result = dao.deleteShadowThing(THING_NAME, shadowName); //NOPMD
        assertThat("Correct payload returned", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(expectedPayload)));

        Optional<byte[]> getResults = dao.getShadowThing(THING_NAME, shadowName);
        assertThat("Could not GET deleted shadow", getResults.isPresent(), is(false));
    }

    @Test
    void GIVEN_no_shadow_WHEN_delete_shadow_thing_THEN_return_nothing() throws Exception {
        Optional<byte[]> result = dao.deleteShadowThing(THING_NAME, SHADOW_NAME);
        assertThat("No shadow found", result.isPresent(), is(false));
    }

    private static Stream<Arguments> namedShadowUpdateSupport() {
        return Stream.of(
                arguments(SHADOW_NAME, CLASSIC_SHADOW_IDENTIFIER, NO_SHADOW_NAME_BASE_DOCUMENT),
                arguments(CLASSIC_SHADOW_IDENTIFIER, SHADOW_NAME, BASE_DOCUMENT)
        );
    }

    @ParameterizedTest
    @MethodSource("namedShadowUpdateSupport")
    void GIVEN_named_and_classic_shadow_WHEN_update_shadow_thing_THEN_correct_shadow_updated(String shadowName, String ignoredShadowName, byte[] ignoredPayload) throws Exception {
        createNamedShadow();
        createClassicShadow();
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT); //NOPMD
        assertThat("Updated shadow", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(UPDATED_DOCUMENT)));

        // Verify we can get the new document
        result = dao.getShadowThing(THING_NAME, shadowName);
        assertThat("Can GET updated shadow", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(UPDATED_DOCUMENT)));

        // Verify that the original shadow with shadowName has not been updated
        result = dao.getShadowThing(THING_NAME, ignoredShadowName);
        assertThat("Can GET other shadow that was not updated", result.isPresent(), is(true));
        assertThat("Other shadow not updated", result.get(), is(equalTo(ignoredPayload)));
    }

    @Test
    void GIVEN_no_shadow_WHEN_update_shadow_thing_THEN_shadow_created() throws Exception {
        Optional<byte[]> result = dao.updateShadowThing(THING_NAME, SHADOW_NAME, UPDATED_DOCUMENT);
        assertThat("Shadow created", result.isPresent(), is(true));
        assertThat(result.get(), is(equalTo(UPDATED_DOCUMENT)));
    }

    @Test
    void GIVEN_multiple_named_shadows_for_thing_WHEN_list_named_shadows_for_thing_THEN_return_named_shadow_list() throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }

        List<String> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, DEFAULT_OFFSET, DEFAULT_LIMIT);
        assertThat(listShadowResults, is(notNullValue()));
        assertThat(listShadowResults, is(not(empty())));
        assertThat(listShadowResults, is(equalTo(SHADOW_NAME_LIST)));
    }

    @Test
    void GIVEN_classic_and_named_shadows_WHEN_list_named_shadows_for_thing_THEN_return_list_does_not_include_classic_shadow() throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }
        dao.updateShadowThing(THING_NAME, CLASSIC_SHADOW_IDENTIFIER, UPDATED_DOCUMENT);

        List<String> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, DEFAULT_OFFSET, SHADOW_NAME_LIST.size());
        assertThat(listShadowResults, is(notNullValue()));
        assertThat(listShadowResults, is(not(empty())));
        assertThat(listShadowResults, is(equalTo(SHADOW_NAME_LIST)));
        assertThat(listShadowResults.size(), is(equalTo(SHADOW_NAME_LIST.size())));
    }

    @Test
    void GIVEN_offset_and_limit_WHEN_list_named_shadows_for_thing_THEN_return_named_shadow_subset() throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }

        int offset = 1;
        int limit = 2;
        List<String> listShadowResults = dao.listNamedShadowsForThing(THING_NAME, offset, limit);
        List<String> expected_paginated_list = Arrays.asList("bravo", "charlie");
        assertThat(listShadowResults, is(notNullValue()));
        assertThat(listShadowResults, is(not(empty())));
        assertThat(listShadowResults, is(equalTo(expected_paginated_list)));
    }

    static Stream<Arguments> validListTestParameters() {
        return Stream.of(
                arguments(THING_NAME, 0, 5),   // limit greater than number of named shadows
                arguments(THING_NAME, 0, 2),   // limit is less than number of named shadows
                arguments(THING_NAME, 0, -10), // limit is negative
                arguments(THING_NAME, 4, 5),   // offset is equal to or greater than number of named shadows
                arguments(THING_NAME, -10, 5), // offset is negative
                arguments(MISSING_THING_NAME, 0, 5),  // list for thing that does not exist
                arguments(CLASSIC_SHADOW_THING, 0, 5) // list for thing that does not have named shadows
        );
    }

    @ParameterizedTest
    @MethodSource("validListTestParameters")
    void GIVEN_valid_edge_inputs_WHEN_list_named_shadows_for_thing_THEN_return_valid_results(String thingName, int offset, int pageSize) throws Exception {
        for (String shadowName : SHADOW_NAME_LIST) {
            dao.updateShadowThing(THING_NAME, shadowName, UPDATED_DOCUMENT);
        }

        dao.updateShadowThing(CLASSIC_SHADOW_THING, CLASSIC_SHADOW_IDENTIFIER, UPDATED_DOCUMENT);

        List<String> listShadowResults = dao.listNamedShadowsForThing(thingName, offset, pageSize);
        assertThat(listShadowResults, is(notNullValue()));

        // cases where valid results are empty (missing thing, thing with no named shadows, offset greater/equal to number of named shadows)
        if (thingName.equals(MISSING_THING_NAME)
                || thingName.equals(CLASSIC_SHADOW_THING)
                || offset >= SHADOW_NAME_LIST.size()) {
            assertThat(listShadowResults, is(empty()));
        }

        // cases where offset and limit are ignored (offset/limit are negative)
        if (offset < 0 || pageSize < 0) {
            assertThat("Original results remained the same", listShadowResults, is(equalTo(SHADOW_NAME_LIST)));
        }
    }
}