package tmnt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase api操作 增删改查
 */
public class HbaseDao {
    private Configuration configuration;
    private HBaseAdmin admin;

    /**
     * 初始化HBaseAdmin 对HBase操作由此 主要操作表 创建 修改表 删除表
     * HBaseConfiguration对HBase进行配置
     *
     * @throws IOException
     */
    public HbaseDao() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.43.123");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        admin = new HBaseAdmin(configuration);
    }

    public boolean isTableExits(String tableName) throws IOException {
        return admin.tableExists(tableName);
    }

    /**
     * 创建表
     *
     * @param table
     * @param colFamily 传入列族 由HTableDescriptor传入
     * @throws IOException
     */
    public void createTable(String table, List<String> colFamily) throws IOException {
        if (isTableExits(table)) {
            System.out.println("table already exit");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(table));
            for (String col : colFamily) {
                descriptor.addFamily(new HColumnDescriptor(col));
            }
            admin.createTable(descriptor);
        }

    }

    /**
     * 先将表声明不可用
     *
     * @param table
     * @throws IOException
     */
    public void deleteTable(String table) throws IOException {
        if (!isTableExits(table)) {
            System.out.println("table is not exit");
        } else {
            admin.disableTable(table);
            admin.deleteTable(table);
        }
    }

    /**
     * 添加一行数据 用put将数据添加到HBase
     * 将数据的操作交由HTable来进行
     *
     * @param table     表名
     * @param rowKey    指定行
     * @param colFamily 指定列族
     * @param col       指定列
     * @param value     值
     * @throws IOException
     */
    public void addRowData(String table, String rowKey, String colFamily, String col, String value) throws IOException {
        HTable hTable = new HTable(configuration, table);
        Put put = new Put(rowKey.getBytes());
        put.add(colFamily.getBytes(), col.getBytes(), value.getBytes());
        hTable.put(put);
        hTable.close();

    }

    /**
     * 删除多行
     *
     * @param table 表名
     * @param rows  指定多行
     * @throws IOException
     */
    public void deleteMultiRow(String table, List<String> rows) throws IOException {
        HTable hTable = new HTable(configuration, table);
        List<Delete> deletes = new ArrayList<Delete>();
        for (int i = 0; i < rows.size() - 1; i++) {
            deletes.add(new Delete(rows.get(i).getBytes()));
        }
        hTable.delete(deletes);
    }

    /**
     * 获取所有数据 要声明ResultScanner 将数据全部取出 进行遍历
     * 得到Result 此用Cell封装了每行数据
     * Result继承Iterable 可遍历Cell
     *
     * @param table
     * @throws IOException
     */
    public void getAllData(String table) throws IOException {
        HTable hTable = new HTable(configuration, table);
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            print(cells);
        }
    }

    /**
     * 获取一行数据 与上同
     * <p>
     * 取一行用Get 传入HTable.get()
     *
     * @param table
     * @param rowKey
     * @throws IOException
     */
    public void getRowData(String table, String rowKey) throws IOException {
        HTable hTable = new HTable(configuration, table);
        Get get = new Get(rowKey.getBytes());
        Result result = hTable.get(get);
        List<Cell> cells = result.listCells();
        print(cells);
    }


    /**
     * 按列族 列标示取数据
     *
     * @param table
     * @param rowKey    指定列
     * @param colFamily 列族
     * @param qualifier 列标识
     * @throws IOException
     */
    public void getRowQualifier(String table, String rowKey, String colFamily, String qualifier) throws IOException {
        HTable hTable = new HTable(configuration, table);
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(), qualifier.getBytes());
        Result result = hTable.get(get);
        List<Cell> cells = result.listCells();
        print(cells);
    }

    public void print(List<Cell> cells) {
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "-----"
                    + Bytes.toString(CellUtil.cloneRow(cell)) + "-----"
                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + "----"
                    + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }
}
