package de.m3y.hadoop.hdfs.hfsa.core;

import de.m3y.hadoop.hdfs.hfsa.util.FsUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static de.m3y.hadoop.hdfs.hfsa.core.FsImageData.ROOT_PATH;
import static de.m3y.hadoop.hdfs.hfsa.util.FsUtil.isFile;
import static de.m3y.hadoop.hdfs.hfsa.util.FsUtil.isSymlink;

/**
 * Visitor for all files and directories.
 *
 * @see Builder
 */
public interface FsVisitorExtended {
  /**
   * Invoked for each file.
   *
   * @param inode the file inode.
   * @param path  the current path.
   */
  void onFile(FsImageProto.INodeSection.INode inode, FsImageProto.INodeSection.INode parent, String path);

  /**
   * Invoked for each directory.
   *
   * @param inode the directory inode.
   * @param path  the current path.
   */
  void onDirectory(FsImageProto.INodeSection.INode inode, FsImageProto.INodeSection.INode parent, String path);

  /**
   * Invoked for each sym link.
   *
   * @param inode the sym link inode.
   * @param path  the current path.
   */
  void onSymLink(FsImageProto.INodeSection.INode inode, FsImageProto.INodeSection.INode parent, String path);

  void onPostVisit(FsImageProto.INodeSection.INode inode, FsImageProto.INodeSection.INode parent, String path);
  void onPreVisit(FsImageProto.INodeSection.INode inode, FsImageProto.INodeSection.INode parent, String path);

  /**
   * Builds a visitor with single-threaded (default) or parallel execution.
   * <p>
   * Builder is immutable and creates a new instance if changed.
   */
  class Builder {
    private static final Logger LOG = LoggerFactory.getLogger(
        FsVisitorExtended.Builder.class);
    public static final FsVisitorStrategy PARALLEL_STRATEGY = new FsVisitorParallelExtendedStrategy();
    public static final FsVisitorStrategy DEFAULT_STRATEGY = PARALLEL_STRATEGY;

    private final FsVisitorStrategy fsVisitorStrategy;

    /**
     * Default constructor.
     */
    public Builder() {
      this(DEFAULT_STRATEGY);
    }

    /**
     * Copy constructor
     *
     * @param fsVisitorStrategy the strategy to visit eg in parallel.
     */
    protected Builder(FsVisitorStrategy fsVisitorStrategy) {
      this.fsVisitorStrategy = fsVisitorStrategy;
    }

    public Builder parallel() {
      return new Builder(PARALLEL_STRATEGY);
    }

    public void visit(FsImageData fsImageData, FsVisitorExtended visitor) throws IOException {
      fsVisitorStrategy.visit(fsImageData, visitor);
    }

    public void visit(FsImageData fsImageData, FsVisitorExtended visitor, String path) throws IOException {
      fsVisitorStrategy.visit(fsImageData, visitor, path);
    }

    interface FsVisitorStrategy {
      void visit(FsImageData fsImageData, FsVisitorExtended visitor) throws IOException;

      void visit(FsImageData fsImageData, FsVisitorExtended visitor, String path) throws IOException;
    }

    public static class FsVisitorParallelExtendedStrategy implements FsVisitorStrategy {

      /**
       * Traverses FS tree, using Java parallel stream.
       *
       * @param visitor the visitor.
       * @throws IOException on error.
       */
      public void visit(FsImageData fsImageData, FsVisitorExtended visitor) throws IOException {
        visit(fsImageData, visitor, ROOT_PATH);
      }

      /**
       * Traverses FS tree, using Java parallel stream.
       *
       * @param visitor the visitor.
       * @param path    the directory path to start with
       * @throws IOException on error.
       */
      public void visit(FsImageData fsImageData, FsVisitorExtended visitor, String path) throws IOException {
        FsImageProto.INodeSection.INode rootNode = fsImageData.getINodeFromPath(path);
        visitor.onPreVisit(rootNode, null, path);
        visitor.onDirectory(rootNode, null, path);
        final long rootNodeId = rootNode.getId();
        final long[] children = fsImageData.getChildINodeIds(rootNodeId);
        if (children.length>0) {
          List<FsImageProto.INodeSection.INode> dirs = new ArrayList<>();
          for (long cid : children) {
            final FsImageProto.INodeSection.INode inode = fsImageData.getInode(cid);
            if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
              dirs.add(inode);
            } else {
              visit(fsImageData, visitor, inode, rootNode, path);
            }
          }

          // Go over top level dirs in parallel
          dirs.parallelStream().forEach(inode -> {
            try {
              visit(fsImageData, visitor, inode, rootNode, path);
            } catch (IOException e) {
              LOG.error("Can not traverse {} : {}", inode.getId(), inode.getName().toStringUtf8(), e);
            }
          });
        }
        visitor.onPostVisit(rootNode, null, path);
      }

      void visit(FsImageData fsImageData, FsVisitorExtended visitor, FsImageProto.INodeSection.INode inode, FsImageProto.INodeSection.INode parent, String path) throws IOException {
        visitor.onPreVisit(inode, parent, path);
        if (FsUtil.isDirectory(inode)) {
          visitor.onDirectory(inode, parent, path);
          final long inodeId = inode.getId();
          final long[] children = fsImageData.getChildINodeIds(inodeId);
          if (children.length > 0) {
            String newPath;
            if (ROOT_PATH.equals(path)) {
              newPath = path + inode.getName().toStringUtf8();
            } else {
              newPath = path + '/' + inode.getName().toStringUtf8();
            }
            for (long cid : children) {
              visit(fsImageData, visitor, fsImageData.getInode(cid), inode, newPath);
            }
          }
        } else if (isFile(inode)) {
          visitor.onFile(inode, parent, path);
        } else if (isSymlink(inode)) {
          visitor.onSymLink(inode, parent, path);
        } else {
          throw new IllegalStateException("Unsupported inode type " + inode.getType() + " for " + inode);
        }
        visitor.onPostVisit(inode, parent, path);
      }
    }
  }
}
