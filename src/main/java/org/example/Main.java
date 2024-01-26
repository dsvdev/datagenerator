package org.example;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Stream;

public class Main {
    private static ObjectWriter objectWriter;
    private static Random random = new Random();

    public static void main(String[] args) {
//        Path include = Path.of("big_include.csv");
        Path exclude = Path.of("big_exclude.csv");
//        Path include2 = Path.of("big_include2.csv");
//        Path exclude2 = Path.of("big_exclude2.csv");
        System.out.println("Starting generation of data");
        List<ClientHash> includeHashes = generateClientHashes(20000000, HashType.MD5, List.of("177780a7f84405f190cfc55ca8b69faa"));
//        List<ClientHash> excludeHashes2 = generateClientHashes(20000000, HashType.SHA512, List.of("886ed4aca7897de63453d8132fe67c1e173600814ddf433acd9476d9b6a8e0055c037d0dbac47ef21fe0f41fa2cb5243a3f5d8b3efb71e0bd9eb9861d18fc476"));
        System.out.println("Finished generation of data");
        System.out.println("Starting writing data to files");

        var csvMapper = CsvMapper.builder()
                .enable(JsonGenerator.Feature.IGNORE_UNKNOWN)
                .enable(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS)
                .build();
        csvMapper.disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
        var csvSchema = csvMapper.schemaFor(ClientHash.class).withHeader();
        objectWriter = csvMapper.writer(csvSchema);

        writeDataToFile(exclude, includeHashes.stream());
//        writeDataToFile(exclude, excludeHashes.stream());
    }

    private static String randomString() {
        return UUID.randomUUID().toString();
    }

    private static List<ClientHash> generateClientHashes(long count, HashType hashType) {
        return generateClientHashes(count, hashType, Collections.emptyList());
    }

    private static List<ClientHash> generateClientHashes(long count, HashType hashType, List<String> includedData) {
        List<ClientHash> clientHashes = new ArrayList<>();
        MessageDigest messageDigest = null;
        try {
            switch (hashType) {
                case MD5:
                    messageDigest = MessageDigest.getInstance("MD5");
                    break;
                case SHA512:
                    messageDigest = MessageDigest.getInstance("SHA-512");
                    break;
                default:
                    throw new RuntimeException("Unknown hash type");
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        for(String data : includedData) {
            clientHashes.add(new ClientHash(data));
        }

        for(long i = includedData.size(); i < count; i++) {
            messageDigest.update(randomString().getBytes());
            clientHashes.add(new ClientHash(Base64.getEncoder().encodeToString(messageDigest.digest())));
        }
        return clientHashes;
    }

    private static Optional<Path> writeDataToFile(Path file, Stream<ClientHash> objectStream) {
        int count = 0;
        try (
                var fileOutputStream = Files.newOutputStream(file);
                var sequenceWriter = objectWriter.writeValues(fileOutputStream)
        ) {
            objectStream.forEach(hash -> writeHash(sequenceWriter, hash));
            return Optional.of(file);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return Optional.empty();
        }
    }

    private static void writeHash(SequenceWriter sequenceWriter, ClientHash transaction) {
        try {
            sequenceWriter.write(transaction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}