package net.intelie.disq;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexFileTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private File indexPath;
    private IndexFile index;

    @Before
    public void setUp() throws Exception {
        indexPath = temp.newFile();
        index = new IndexFile(indexPath.toPath());
    }

    @Test
    public void willHaveDefaults() throws Exception {
        assertThreeFirst(index, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void willSave() throws Exception {
        exampleData(index);

        assertThat(indexPath.length()).isEqualTo(0);
        index.flush();
        assertThat(indexPath.length()).isEqualTo(512);

        DataInputStream data = new DataInputStream(new FileInputStream(indexPath));
        assertThat(data.readShort()).isEqualTo((short)1);
        assertThat(data.readShort()).isEqualTo((short)2);
        assertThat(data.readInt()).isEqualTo(50);
        assertThat(data.readInt()).isEqualTo(82);
        assertThat(data.readLong()).isEqualTo(1);
        assertThat(data.readLong()).isEqualTo(83);

        assertThat(data.readInt()).isEqualTo(0);
        assertThat(data.readInt()).isEqualTo(-1);
        assertThat(data.readInt()).isEqualTo(2);
        for (int i = 3; i < IndexFile.MAX_FILES; i++) {
            assertThat(data.readInt()).isEqualTo(0);
        }
        assertThat(data.read()).isEqualTo(-1);
    }

    private void exampleData(IndexFile index) {
        index.addReadCount(2);
        index.addWriteCount(5);
        index.advanceWriteFile();
        index.advanceWriteFile();
        index.advanceReadFile(4);
        index.addReadCount(50);
        index.addWriteCount(56);
        index.addWriteCount(26);
    }

    @Test
    public void testSaveAndReopen() throws Exception {
        exampleData(index);

        assertThat(indexPath.length()).isEqualTo(0);
        index.flush();
        index.close();

        index = new IndexFile(indexPath.toPath());

        assertThreeFirst(index, 1, 2, 50, 82, 1, 83, 0, -1, 2);
    }

    private void assertThreeFirst(IndexFile index, int readFile, int writeFile, int readPosition, int writePosition, int count, int bytes, int c1, int c2, int c3) {
        assertThat(index.getReadFile()).isEqualTo(readFile);
        assertThat(index.getWriteFile()).isEqualTo(writeFile);
        assertThat(index.getReadPosition()).isEqualTo(readPosition);
        assertThat(index.getWritePosition()).isEqualTo(writePosition);
        assertThat(index.getCount()).isEqualTo(count);
        assertThat(index.getBytes()).isEqualTo(bytes);

        assertThat(index.getFileCount(0)).isEqualTo(c1);
        assertThat(index.getFileCount(1)).isEqualTo(c2);
        assertThat(index.getFileCount(2)).isEqualTo(c3);
        for (int i = 3; i < IndexFile.MAX_FILES; i++) {
            assertThat(index.getFileCount(i)).isEqualTo(0);
        }
    }

    @Test
    public void testClear() throws Exception {
        exampleData(index);

        assertThat(indexPath.length()).isEqualTo(0);
        index.flush();
        index.clear();
        index.flush();
        index.close();

        index = new IndexFile(indexPath.toPath());

        assertThreeFirst(index, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test
    public void assertAdvanceWrite() throws Exception {
        index.addWriteCount(42);

        assertThat(index.getWritePosition()).isEqualTo(42);
        assertThat(index.getBytes()).isEqualTo(42);
        assertThat(index.getCount()).isEqualTo(1);
        assertThat(index.getFileCount(0)).isEqualTo(1);
    }

    @Test
    public void assertAdvanceRead() throws Exception {
        index.addReadCount(42);

        assertThat(index.getReadPosition()).isEqualTo(42);
        assertThat(index.getBytes()).isEqualTo(0);
        assertThat(index.getCount()).isEqualTo(-1);
        assertThat(index.getFileCount(0)).isEqualTo(-1);
    }

    @Test
    public void assertAdvanceFileWrite() throws Exception {
        index.addWriteCount(42);
        index.advanceWriteFile();

        assertThat(index.getWritePosition()).isEqualTo(0);
        assertThat(index.getWriteFile()).isEqualTo(1);
        assertThat(index.getBytes()).isEqualTo(42);
        assertThat(index.getCount()).isEqualTo(1);
        assertThat(index.getFileCount(0)).isEqualTo(1);
        assertThat(index.getFileCount(1)).isEqualTo(0);
    }

    @Test
    public void assertAdvanceFileRead() throws Exception {
        index.addWriteCount(42);
        index.addWriteCount(42);
        index.addReadCount(42);
        index.advanceReadFile(84);

        assertThat(index.getReadPosition()).isEqualTo(0);
        assertThat(index.getBytes()).isEqualTo(0);
        assertThat(index.getCount()).isEqualTo(0);
        assertThat(index.getFileCount(0)).isEqualTo(0);
        assertThat(index.getFileCount(1)).isEqualTo(0);
    }

    @Test
    public void testIsInUse() throws Exception {
        for (int i = 0; i < 10; i++)
            index.advanceWriteFile();

        for (int i = 0; i < 5; i++)
            index.advanceReadFile(0);

        for (int i = 0; i < 5; i++)
            assertThat(index.isInUse(i)).isFalse();
        for (int i = 5; i <= 10; i++)
            assertThat(index.isInUse(i)).isTrue();
        for (int i = 11; i < 20; i++)
            assertThat(index.isInUse(i)).isFalse();
    }

    @Test
    public void testIsInUseInverse() throws Exception {
        for (int i = 0; i < 10; i++)
            index.advanceWriteFile();

        for (int i = 0; i < 10; i++)
            index.advanceReadFile(0);

        for (int i = 0; i < IndexFile.MAX_FILES - 5; i++)
            index.advanceWriteFile();

        for (int i = 0; i <= 5; i++)
            assertThat(index.isInUse(i)).isTrue();
        for (int i = 6; i < 10; i++)
            assertThat(index.isInUse(i)).isFalse();
        for (int i = 10; i < 20; i++)
            assertThat(index.isInUse(i)).isTrue();

    }
}