package lab.nice.nifi.invoker.util;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class LobHandler implements AutoCloseable {
    private static final int DEFAULT_BUFFER_SIZE = 256;

    private final Statement statement;
    private final List<Clob> clobs;
    private final List<NClob> nClobs;

    public LobHandler(final Statement statement) {
        this.statement = statement;
        this.clobs = new ArrayList<>();
        this.nClobs = new ArrayList<>();
    }

    public Clob clob(final Reader reader) throws SQLException, IOException {
        if (null == reader) {
            return null;
        } else {
            final Clob clob = statement.getConnection().createClob();
            writeToClob(clob, reader);
            return clob;
        }
    }

    public NClob nClob(final Reader reader) throws SQLException, IOException {
        if (null == reader) {
            return null;
        } else {
            final NClob nClob = statement.getConnection().createNClob();
            writeToClob(nClob, reader);
            return nClob;
        }
    }

    private void writeToClob(final Clob clob, final Reader reader) throws IOException, SQLException {
        final CharBuffer charBuffer = CharBuffer.allocate(DEFAULT_BUFFER_SIZE);
        long position = 1;
        while (reader.read(charBuffer) != -1) {
            charBuffer.flip();
            clob.setString(position, charBuffer.toString());
            position = position + charBuffer.limit();
            charBuffer.clear();
        }
    }

    @Override
    public void close() throws SQLException {
        for (Clob clob : clobs) {
            clob.free();
        }
        for (NClob nClob : nClobs) {
            nClob.free();
        }
    }
}
