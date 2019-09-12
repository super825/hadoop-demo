package com.ada.reducejoin;

import com.ada.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RJComparator extends WritableComparator {

    protected RJComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}
