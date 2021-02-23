package de.m3y.hadoop.hdfs.hfsa.tool;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitorExtended;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * Reports details about an inode structure.
 */
@CommandLine.Command(name = "path", aliases = "p",
    description = "Lists INode paths",
    mixinStandardHelpOptions = true,
    helpCommand = true,
    showDefaultValues = true
)
public class CSVReportCommand extends AbstractReportCommand {

  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm";
  private static final DateTimeFormatter fmt = DateTimeFormatter.ofPattern(DATE_FORMAT);

  @Override
  public void run() {
    final FsImageData fsImageData = loadFsImage();
    if (null != fsImageData) {
      createReport(fsImageData);
    }
  }

  private void createReport(FsImageData fsImageData) {
    try {
      INodePredicate predicate;
      if (null != mainCommand.userNameFilter) {
        Pattern userPattern = Pattern.compile(mainCommand.userNameFilter);
        predicate = new INodePredicate() {
          @Override
          public String toString() {
            return "user=~" + userPattern;
          }

          @Override
          public boolean test(INode iNode, String path) {
            final PermissionStatus permissionStatus =
                fsImageData.getPermissionStatus(iNode);
            return userPattern.matcher(permissionStatus.getUserName())
                .matches();
          }
        };
      } else {
        predicate = new INodePredicate() {
          @Override
          public boolean test(INode iNode, String path) {
            return true;
          }

          @Override
          public String toString() {
            return "no filter";
          }
        };
      }

      final CSVPrinter printer = new CSVPrinter(Files.newBufferedWriter(this.mainCommand.outputPath), CSVFormat.DEFAULT);
      final CSVVisitor visitor = new CSVVisitor(fsImageData, mainCommand.out, predicate, printer);
      final FsVisitorExtended.Builder builder = new FsVisitorExtended.Builder().parallel();

      printer.printRecord(CSVVisitor.getHeader());

      for (String dir : mainCommand.dirs) {
        builder.visit(fsImageData,
            visitor,
            dir);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @FunctionalInterface
  public interface INodePredicate {
    boolean test(INode iNode, String path);
  }

  static class AccumulatedResult {
    long size;
    long filesCount;
    long dirsCount;
    long blocksCount;

    public AccumulatedResult(long size, long filesCount, long dirsCount,
        long blocksCount) {
      this.size = size;
      this.filesCount = filesCount;
      this.dirsCount = dirsCount;
      this.blocksCount = blocksCount;
    }

    public AccumulatedResult() {
      this(0, 0, 0, 0);
    }

    public AccumulatedResult merge(AccumulatedResult other) {
      return new AccumulatedResult(this.size + other.size,
          this.filesCount + other.filesCount,
          this.dirsCount + other.dirsCount,
          this.blocksCount + other.blocksCount);
    }
  }

  static class CSVVisitor implements FsVisitorExtended {
    final INodePredicate predicate;

    final PrintStream out;
    final CSVPrinter printer;
    final FsImageData fsImageData;
    final Map<Long, AccumulatedResult> accumulatedResults = new HashMap<>();

    CSVVisitor(FsImageData fsImageData, PrintStream out, INodePredicate predicate, CSVPrinter printer) {
      this.fsImageData = fsImageData;
      this.out = out;
      this.predicate = predicate;
      this.printer = printer;
    }

    @Override
    public void onFile(INode inode, INode parent, String path) {

    }

    @Override
    public void onDirectory(INode inode, INode parent, String path) {

    }

    @Override
    public void onSymLink(INode inode, INode parent, String path) {

    }

    @Override
    public void onPreVisit(INode inode, INode parent, String path) {
      if (inode.hasDirectory()) {
        synchronized (this.accumulatedResults) {
          accumulatedResults.put(inode.getId(), new AccumulatedResult(0, 0, 1, 0));
        }
      }
    }

    @Override
    public void onPostVisit(INode inode, INode parent, String path) {
      AccumulatedResult iNodeAccResult;

      if (inode.hasDirectory()) {
        synchronized (this.accumulatedResults) {
          iNodeAccResult = accumulatedResults.get(inode.getId());
        }
      } else if (inode.hasFile()) {
        FsImageProto.INodeSection.INodeFile f = inode.getFile();
        iNodeAccResult =
            new AccumulatedResult(getFileSize(f), 1, 0, f.getBlocksCount());
      } else {
        iNodeAccResult = new AccumulatedResult();
      }

      // Update parent
      // Root will have a null parent.
      if (parent != null) {
        long parentId = parent.getId();
        synchronized (this.accumulatedResults) {
          accumulatedResults.replace(parentId,
              accumulatedResults.get(parentId).merge(iNodeAccResult));
        }
      }

      reportINode(inode, iNodeAccResult, path);
    }

    private void reportINode(INode iNode, AccumulatedResult iNodeAccResult, String path) {

      Iterable<Object> formattedEntry = getEntry(iNode, iNodeAccResult, path);

      synchronized (this.printer) {
        try {
          printer.printRecord(formattedEntry);

          // Root is always last to post-visit.
          if(path.equals(FsImageData.ROOT_PATH)) {
            printer.flush();
          }
        } catch (IOException exception) {
          exception.printStackTrace();
        }
      }
    }

    public Iterable<Object> getEntry(INode iNode, AccumulatedResult iNodeAccResult, String path) {
      List<Object> buf = new ArrayList<>();

      char iNodeType = '-';

      long iNodeId = iNode.getId();

      final String iNodeName = iNode.getName().toStringUtf8();
      final String absolutPath = path.length() > 1 ? path + '/' + iNodeName : path + iNodeName;

      long permission = fsImageData.getPermission(iNode);
      PermissionStatus p = fsImageData.getPermissionStatus(permission);

      switch (iNode.getType()) {
        case FILE:
          FsImageProto.INodeSection.INodeFile file = iNode.getFile();
          buf.add(absolutPath);
          buf.add(file.getReplication());
          buf.add(formatDate(file.getModificationTime()));
          buf.add(formatDate(file.getAccessTime()));
          buf.add(file.getPreferredBlockSize());
          buf.add(iNodeAccResult.blocksCount);
          buf.add(iNodeAccResult.size);
          buf.add(0);
          buf.add(0);
          buf.add(iNodeType + p.getPermission().toString());
          buf.add(p.getUserName());
          buf.add(p.getGroupName());
          buf.add(iNodeId);
          buf.add(new Path(path).depth());
          buf.add(1);
          buf.add(0);
          break;
        case DIRECTORY:
          FsImageProto.INodeSection.INodeDirectory dir = iNode.getDirectory();
          iNodeType = 'd';
          buf.add(absolutPath);
          buf.add(0);
          buf.add(formatDate(dir.getModificationTime()));
          buf.add(formatDate(0));
          buf.add(0);
          // blocks count
          buf.add(iNodeAccResult.blocksCount);
          // size
          buf.add(iNodeAccResult.size);
          buf.add(dir.getNsQuota());
          buf.add(dir.getDsQuota());
          buf.add(iNodeType + p.getPermission().toString());
          buf.add(p.getUserName());
          buf.add(p.getGroupName());
          buf.add(iNodeId);
          buf.add(new Path(path).depth());
          buf.add(iNodeAccResult.filesCount);
          buf.add(iNodeAccResult.dirsCount);
          break;
        case SYMLINK:
          FsImageProto.INodeSection.INodeSymlink s = iNode.getSymlink();
          iNodeType = 'l';
          buf.add(absolutPath);
          buf.add(0);
          buf.add(formatDate(s.getModificationTime()));
          buf.add(formatDate(s.getModificationTime()));
          buf.add(0);
          buf.add(0);
          buf.add(0);
          buf.add(0);
          buf.add(0);
          buf.add(iNodeType + p.getPermission().toString());
          buf.add(p.getUserName());
          buf.add(p.getGroupName());
          buf.add(iNodeId);
          buf.add(new Path(path).depth());
          buf.add(0);
          buf.add(0);
          break;
        default:
          break;
      }

      return buf;
    }

    public static Iterable<Object> getHeader() {
      String[] headers = new String[] {
          "Path",
          "Replication",
          "ModificationTime",
          "AccessTime",
          "PreferredBlockSize",
          "BlocksCount",
          "FileSize",
          "NSQUOTA",
          "DSQUOTA",
          "Permission",
          "UserName",
          "GroupName",
          "InodeId",
          "Depth",
          "FilesCount",
          "DirsCount"
      };

      return Arrays.asList(headers);
    }

    private static String formatDate(long seconds) {
      return Instant.ofEpochMilli(seconds).atOffset(ZoneOffset.UTC).toLocalDateTime().format(fmt);
    }

    private static long getFileSize(FsImageProto.INodeSection.INodeFile f) {
      long size = 0;
      for (HdfsProtos.BlockProto p : f.getBlocksList()) {
        size += p.getNumBytes();
      }
      return size;
    }
  }
}
