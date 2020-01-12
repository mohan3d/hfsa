package de.m3y.hadoop.hdfs.hfsa.generator;


import java.io.File;
import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImage;

/**
 * Generates an FSImage for testing.
 */
public class FSImageGenerator {
    static String abc = "abcdefghijklmnopqrstuvwxyz";

    public static void main(String[] args) throws IOException {
        /* How deep aka directory levels  */
        final int maxDepth = Math.min(5, abc.length());
        /* How many children per depth level and node */
        final int maxWidth = Math.min(2, abc.length());
        /* "a...z".length * factor = number of files per directory generated */
        final int filesPerDirectoryFactor = 10;

        /**
         * #dirs = (1-maxWidth**maxDepth)/(1-maxWidth) * 26
         * #files = #dirs * 26 * filesPerDirectoryFactor
         *
         * 5/6/10 => 20306 directories and 5279560 files, ~270MiB
         * 5/3/10 => 3146 directories and 817960 files, ~40MiB
         * 5/2/10 => 3146 directories and 817960 files, ~40MiB
         */
        System.out.println("Max depth = " + maxDepth + ", max width = " + maxWidth + ", files-factor = " + filesPerDirectoryFactor);
        int eDirs = abc.length() * (1 - Double.valueOf(Math.rint(Math.pow(maxWidth, maxDepth))).intValue()) / (1 - maxWidth);
        System.out.println("Generates " + eDirs + " dirs and " + eDirs * abc.length() * filesPerDirectoryFactor + " files");

        HdfsConfiguration conf = new HdfsConfiguration();
        try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build()) {
            long dirCounter = 0;
            long fileCounter = 0;

            try (DistributedFileSystem dfs = cluster.getFileSystem()) {
                Stack<String> stack = new Stack<>();
                abc.chars().forEach(c -> stack.push("/" + (char) c));

                while (!stack.isEmpty()) {
                    String s = stack.pop();
                    final Path path = new Path(s);
                    if (dirCounter % 100 == 0) {
                        System.out.println(path);
                        System.out.println("Created " + dirCounter + " directories and " + fileCounter + " files.");
                    }
                    dfs.mkdirs(path);
                    dirCounter++;
                    for (int i = 0; i < abc.length(); i++) {
                        for (int c = 0; c < filesPerDirectoryFactor; c++) {
                            dfs.createNewFile(new Path(path, "" + abc.charAt(i) + "_" + c));
                            fileCounter++;
                        }
                    }
                    final String name = path.getName();
                    if (name.length() < maxDepth) {
                        for (int i = 0; i < maxWidth /* Limit width for depth */; i++) {
                            stack.push(s + "/" + name + abc.charAt(i));
                        }
                    }
                }
                dfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
                dfs.saveNamespace();
                final FSImage fsImage = cluster.getNameNode().getFSImage();
                final File highestFsImageName = fsImage.getStorage().getHighestFsImageName();
                File newFsImage = new File("fsimage.img");
                if (newFsImage.exists()) {
                    newFsImage.delete();
                }
                highestFsImageName.renameTo(newFsImage);
                System.out.println("Created " + dirCounter + " directories and " + fileCounter
                        + " files in fsimage " + newFsImage.getAbsolutePath());
            }
        }
    }
}