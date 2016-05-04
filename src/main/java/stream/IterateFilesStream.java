package stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import stream.annotations.Parameter;
import stream.io.AbstractStream;
import stream.io.SourceURL;
import stream.io.multi.AbstractMultiStream;

/**
 * @author alexey
 */
public class IterateFilesStream extends AbstractMultiStream {

    static Logger log = LoggerFactory.getLogger(IterateFilesStream.class);

    @Parameter(required = true)
    String ending = "";

    @Parameter(required = false, defaultValue = "true",
            description = "If something goes wrong while reading a file, just continue with the next one.")
    boolean skipErrors = true;

    private ArrayList<FileStatus> fileStatuses;
    private int fileCounter;

    private AbstractStream stream;
    private int failedFilesCounter;
    private ArrayList<String> failedFilesList;
    private int countReadNext;
    private FileSystem fs;

    public IterateFilesStream() {
        super();
    }

    public IterateFilesStream(SourceURL url) {
        super(url);
    }

    /**
     * Filter files in HDFS folder.
     */
    private class HDFSPathFilter extends Configured implements PathFilter {
        Configuration conf;
        Pattern pattern;

        @Override
        public boolean accept(Path path) {
            Matcher matcher = pattern.matcher(path.toString());
            return matcher.matches();
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            pattern = Pattern.compile("^.*" + ending + "$");
        }
    }

    @Override
    public void init() throws Exception {

        // initialize variables
        failedFilesList = new ArrayList<>(0);
        fileCounter = 1;
        countReadNext = 1;

        // retrieve file system that contains all information about HDFS file system
        fs = FileSystem.get(new URI(this.url.toString()), new Configuration());

        // correct setting for working directory
        correctWorkingDirectory(fs);

        String path = this.url.getPath();
        if (!path.endsWith("/")) {
            path += "/";
        }

        // find all files in folder and its subfolders
        fileStatuses = retrieveFilesRecursively(path);
        log.info("Found {} files in the HDFS folder.", this.fileStatuses.size());

        if (stream == null && additionOrder != null) {
            stream = (AbstractStream) streams.get(additionOrder.get(0));
            stream.setUrl(new SourceURL(this.fileStatuses.remove(0).getPath().toString()));
            stream.init();
            log.info("Streaming file {}: {}", fileCounter, stream.getUrl().toString());
            fileCounter++;
        }
    }

    /**
     * Retrieve all files contained in given path and its subfolders.
     *
     * @param path path to the folder containing files and folders
     * @return list of all files in this folder and its subfolders
     */
    private ArrayList<FileStatus> retrieveFilesRecursively(String path) throws IOException {
        ArrayList<FileStatus> result = new ArrayList<>(0);

        // retrieve all files and for folders call this method recursively
        FileStatus[] folders = fs.listStatus(new Path(path));
        for (FileStatus folder : folders) {
            if (folder.isDirectory()) {
                result.addAll(retrieveFilesRecursively(folder.getPath().toString()));
                log.info("Adding files from {}", folder.getPath().toString());
            }
        }

        // add files that match filter
        FileStatus[] filteredFiles = fs.listStatus(new Path(path), new HDFSPathFilter());

        // add found files to the resulted list
        result.addAll(new ArrayList<>(Arrays.asList(filteredFiles)));
        return result;
    }

    /**
     * Change working directory that is expanded by default with "/user/'username'/". If this is the
     * case, than set working directory to a new value using protocol, host and port of a URL.
     *
     * @param fs filesystem
     */
    private void correctWorkingDirectory(FileSystem fs) {
        if (!url.toString().startsWith(fs.getWorkingDirectory().toString())) {
            String host = this.url.getHost();
            int port = this.url.getPort();
            String protocol = this.url.getProtocol();
            String workingDirectory = protocol + "://" + host + ":" + port + "/";
            log.info("\nGiven URL is {}.\nBut working directory is set to {}.\nChanging it to {}",
                    url.toString(), fs.getWorkingDirectory(), workingDirectory);
            fs.setWorkingDirectory(new Path(workingDirectory));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        log.info("Processed {} files while failed to read {} files.", fileCounter, failedFilesCounter);

        // log all skipped files
        if (failedFilesCounter > 0) {
            String failesFiles = "";
            for (String failedFile : failedFilesList) {
                failesFiles += "\n" + failedFile;
            }
            log.info("Some files has been skipped because of errors: {}", failesFiles);
        }
    }

    @Override
    public Data readNext() throws Exception {

        try {
            // try read data item
            Data data = stream.read();

            // if stream has ended, try to start new one
            if (data == null) {
                // get new file
                stream.close();

                // check if some source files are still there,
                // no more files to read -> stop the stream
                if (fileStatuses.size() == 0) {
                    return null;
                }

                stream.setUrl(new SourceURL(fileStatuses.remove(0).getPath().toString()));
                stream.init();

                log.info("Streaming file {}: {}", fileCounter, stream.getUrl().toString());

                fileCounter++;
                data = stream.readNext();
            }

            log.info("Read {} items", countReadNext++);
            return data;

        } catch (IOException e) {
            log.info("File: " + stream.getUrl().toString() + " throws IOException.");

            if (skipErrors) {
                log.info("Skipping broken files. Continuing with next file.");
                stream = null;
                failedFilesCounter++;
                failedFilesList.add(stream.getUrl().toString());
                return this.readNext();
            } else {
                log.error("Stopping stream because of IOException");
                e.printStackTrace();
                stream.close();
                return null;
            }
        }
    }
}
