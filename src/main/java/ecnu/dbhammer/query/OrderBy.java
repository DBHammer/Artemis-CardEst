package ecnu.dbhammer.query;



import ecnu.dbhammer.query.type.OrderByType;
import ecnu.dbhammer.schema.Attribute;
import ecnu.dbhammer.schema.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Order by生成
 */
public class OrderBy {

    private OrderByType orderByType;

    Attribute attribute;


    public OrderBy(List<Table> tables) {

        if (Math.random() < 0.5) {//降序
            this.orderByType = OrderByType.DESC;
            this.attribute = getColumnName4Order(tables);

        } else {
            this.orderByType = OrderByType.ASC;
            this.attribute = getColumnName4Order(tables);
        }
    }

    public OrderBy(Attribute columnName) {
        if (Math.random() < 0.5) {
            this.orderByType = OrderByType.DESC;
            this.attribute = columnName;

        } else {
            this.orderByType = OrderByType.ASC;
            this.attribute = columnName;
        }
    }



    private Attribute getColumnName4Order(List<Table> tables) {
        List<Attribute> attributes = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++) {
            attributes.addAll(tables.get(i).getAllAttrubute());
        }
        int index = (int)(Math.random() * attributes.size());//随机选一个作为order by
        return attributes.get(index);
    }

    public String getAttributeName() {
        return attribute.getFullAttrName();
    }

    public OrderByType getOrderByType() {
        return orderByType;
    }
}
