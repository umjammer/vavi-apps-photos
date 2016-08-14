/*
 * Copyright (c) 2016 by Naohide Sano, All rights reserved.
 *
 * Programmed by Naohide Sano
 */

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.klab.commons.cli.Option;
import org.klab.commons.cli.Options;


/**
 * Exports Photos Library to directory.
 *
 * @author <a href="mailto:umjammer@gmail.com">Naohide Sano</a> (umjammer)
 * @version 0.00 2016/08/12 umjammer initial version <br>
 * @see "https://github.com/tymmej/ExportPhotosLibrary"
 * @see "https://github.com/samrushing/face_extractor"
 * @see "https://github.com/bdwilson/iPhotoDump"
 * @see "https://github.com/namezys/mac_photos"
 */
@Options
public class Main {

    @Option(argName = "verbose", option = "v", description = "increase output verbosity")
    boolean verbose;
    @Option(argName = "source", option = "s", description = "source, path to Photos.app library")
    String source = "/Users/nsano/Pictures/写真 Library.photoslibrary";
    @Option(argName = "destination", option = "d", description = "destination, path to external directory")
    String destination="/Users/nsano/tmp/Photos";
    @Option(argName = "compare", option = "c", description = "compare files")
    boolean compare;
    @Option(argName = "dryrun", option = "n", description = "do not copy files")
    boolean dryrun;
    @Option(argName = "masters", option = "m", description = "export masters instead of edited")
    boolean masters;
    @Option(argName = "links", option = "l", description = "use symlinks")
    boolean links;
    @Option(argName = "hardlinks", option = "i", description = "use hardlinks")
    boolean hardlinks;
    @Option(argName = "progress", option = "p", description = "show progress bar")
    boolean progress = true;

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Main app = new Main();
        Options.Util.bind(args, app);
        app.porcees();
    }
    
    void porcees() throws Exception {
        // options
        if (this.verbose) {
            this.progress = false;
        }
        if (this.progress) {
            this.verbose = false;
        }

        String libraryRoot = this.source;
        String destinationRoot = this.destination;

        if (!new File(destinationRoot).isDirectory()) {
            System.err.println("destination is not a directory?: " + destinationRoot);
            System.exit(-1);
        }

        //copy database, we don't want to mess with original
        Path databasePathLibrary = Files.createTempFile("Library", ".apdb");
        System.err.println(databasePathLibrary);
        Path databasePathEdited = Files.createTempFile("ImageProxies", ".apdb");
        System.err.println(databasePathEdited);
        Files.copy(Paths.get(libraryRoot, "Database", "Library.apdb"), databasePathLibrary, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(libraryRoot, "Database", "ImageProxies.apdb"), databasePathEdited, StandardCopyOption.REPLACE_EXISTING);

        //connect to database
        Connection mainConnection = DriverManager.getConnection("jdbc:sqlite:" + databasePathLibrary);
        PreparedStatement preparedStatement = mainConnection.prepareStatement("attach database ? as L");
        preparedStatement.setString(1, databasePathLibrary.toString());
        preparedStatement.executeUpdate();
        preparedStatement.close();
        Connection proxiesConnection = DriverManager.getConnection("jdbc:sqlite:" + databasePathEdited);
        preparedStatement = proxiesConnection.prepareStatement("attach database ? as L");
        preparedStatement.setString(1, databasePathEdited.toString());
        preparedStatement.executeUpdate();
        preparedStatement.close();

        int images = 0;

        // count all images
        Statement statement = mainConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select RKAlbum.modelid from L.RKAlbum where RKAlbum.albumSubclass = 3");
        while (resultSet.next()) {
            int albumNumber = resultSet.getInt(1);
//            System.err.println("album: " + albumNumber);
            // get all photos in that album
            preparedStatement = mainConnection.prepareStatement("select RKAlbumVersion.VersionId from L.RKAlbumVersion where RKAlbumVersion.albumId = ?");
            preparedStatement.setInt(1, albumNumber);
            ResultSet resultSet2 = preparedStatement.executeQuery();
            while (resultSet2.next()) {
                int versionId = resultSet2.getInt(1);
//                System.err.println("album: " + albumNumber + ", version: " + versionId);
                images += 1;
            }
            resultSet2.close();
            preparedStatement.close();
        }
        resultSet.close();
        statement.close();

        System.err.println("Found " + images + " images");

        int copied = 0;
        int progress = 0;
        int failed = 0;

        // find all "normal" albums
        statement = mainConnection.createStatement();
        resultSet = statement.executeQuery("select RKAlbum.modelid, RKAlbum.name from L.RKAlbum where RKAlbum.albumSubclass = 3");
        while (resultSet.next()) {
            int albumNumber = resultSet.getInt(1);
            String albumName = resultSet.getString(2);
            Path destinationDirectory = Paths.get(destinationRoot, albumName);
            Files.createDirectories(destinationDirectory);
            if (this.verbose) {
                System.err.println("---------------- ALBUM: " + albumName + " ----------------");
            }
            // get all photos in that album
            PreparedStatement preparedStatement2 = mainConnection.prepareStatement("select RKAlbumVersion.VersionId from L.RKAlbumVersion where RKAlbumVersion.albumId = ?");
            preparedStatement2.setInt(1, albumNumber);
            ResultSet resultSet2 = preparedStatement2.executeQuery();
            while (resultSet2.next()) {
                int versionId = resultSet2.getInt(1);
                // get image path/name
                PreparedStatement preparedStatement3 = mainConnection.prepareStatement("select M.imagePath, V.fileName, V.adjustmentUUID from L.RKVersion as V inner join L.RKMaster as M on V.masterUuid=M.uuid where V.modelId = ?");
                preparedStatement3.setInt(1, versionId);
                ResultSet resultSet3 = preparedStatement3.executeQuery();
                while (resultSet3.next()) {
                    progress += 1;
                    if (this.progress) {
                        bar(progress * 100 / images);
                    }
                    String imagePath = resultSet3.getString(1);
                    String fileName = resultSet3.getString(2);
                    String adjustmentUUID = resultSet3.getString(3);
                    Path sourceImage = Paths.get(libraryRoot, "Masters");
                    for (String path : imagePath.split(File.separator)) {
                        sourceImage = sourceImage.resolve(path);
                    }
                    if (!Files.exists(sourceImage)) {
                        System.err.println("ERROR: source file does not exists: " + imagePath);
                    }
                    // copy edited image to destination
                    if (!this.masters) {
                        if (!adjustmentUUID.equals("UNADJUSTEDNONRAW") && !adjustmentUUID.equals("UNADJUSTED")) {
                            PreparedStatement preparedStatement4 = proxiesConnection.prepareStatement("SELECT resourceUuid, filename FROM RKModelResource WHERE resourceTag = ?");
                            preparedStatement4.setString(1, adjustmentUUID);
                            ResultSet resultSet4 = preparedStatement4.executeQuery();
                            if (resultSet4.next()) {
                                String uuid = resultSet4.getString(1);
                                String fileName4 = resultSet4.getString(2);
                                String p1 = String.valueOf((int) uuid.charAt(0));
                                String p2 = String.valueOf((int) uuid.charAt(1));
                                sourceImage = Paths.get(libraryRoot, "resources/modelresources", p1, p2, uuid, fileName4);
                            } else {
                                System.err.println("ERROR: no row for " + adjustmentUUID);
                            }
                            resultSet4.close();
                            preparedStatement4.close();
                        }
                    }
                    Path destinationPath = destinationDirectory.resolve(fileName);
                    if (this.verbose) {
                        System.err.println("\t(" + progress + "/" + images + ") From:\t" + sourceImage + "\tto:\t" + destinationPath);
                    }
                    if (!Files.isRegularFile(destinationPath)) {
                        copied += 1;
                        if (this.verbose) {
                            System.err.println("Copying");
                        }
                        if (!this.dryrun) {
                            try {
                                if (this.links) {
                                    Files.createSymbolicLink(sourceImage, destinationDirectory.resolve(sourceImage.getFileName()));
                                } else if (this.hardlinks) {
                                    Files.createLink(sourceImage, destinationDirectory.resolve(sourceImage.getFileName()));
                                } else {
                                    Files.copy(sourceImage, destinationDirectory);
                                }
                            } catch (IOException e) {
                                failed += 1;
                                System.err.printf("ERROR: failed to copy: %s. skipping this element.\n", sourceImage);
                            }
                        }
                    } else {
                        if (this.verbose) {
                            System.err.print("File already exists");
                            if (this.compare) {
                                if (this.verbose) {
                                    System.err.print("Comparing files");
                                }
                                if (!Files.isSameFile(sourceImage, destinationPath)) {
                                    copied += 1;
                                    if (!this.dryrun) {
                                        if (this.verbose) {
                                            System.err.println("Copying");
                                        }
                                        try {
                                            if (this.links) {
                                                Files.createSymbolicLink(sourceImage, destinationDirectory.resolve(sourceImage.getFileName()));
                                            } else if (this.hardlinks) {
                                                Files.createLink(sourceImage, destinationDirectory.resolve(sourceImage.getFileName()));
                                            } else {
                                                Files.copy(sourceImage, destinationDirectory);
                                            }
                                        } catch (IOException e) {
                                            failed += 1;
                                            System.err.printf("ERROR: failed to copy: %s. skipping this element.", sourceImage);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                resultSet3.close();
                preparedStatement3.close();
            }
            resultSet2.close();
            preparedStatement.close();
        }
        resultSet.close();
        statement.close();

        System.err.println("\nImages:\t" + images + "\tcopied:\t" + copied + "\tfailed:\t" + failed);

        proxiesConnection.close();
        mainConnection.close();

        Files.delete(databasePathLibrary);
        Files.delete(databasePathEdited);

        System.err.println("\nDeleted temporary files");
    }

    void bar(int progress) {
        int i = progress / 5;
        System.err.print("\n");
        System.err.printf("[%-20s] %d%%", new String(new char[i]).replace("\0", "="), progress);
        System.err.print("\n");
        System.err.flush();
    }
}

/* */
