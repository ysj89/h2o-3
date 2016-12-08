package water.rapids.ast.prims.advmath;

import water.AutoBuffer;
import water.MRTask;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.nbhm.NonBlockingHashMapLong;
import water.rapids.Env;
import water.rapids.ast.prims.mungers.AstGroup;
import water.rapids.vals.ValFrame;
import water.rapids.ast.AstPrimitive;
import water.rapids.ast.AstRoot;
import water.util.IcedHashMap;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Near Zero Variance
 */
public class AstNearZeroVariance extends AstPrimitive {
    @Override
    public String[] args() {return new String[]{"ary"}; }

    @Override
    public int nargs() {
        return -1;
    } // nearZeroVar ary

    @Override
    public String str() {
        return "nearZeroVar";
    }

    @Override
    public ValFrame apply(Env env, Env.StackHelp stk, AstRoot asts[]) {
        Frame fr = stk.track(asts[1].exec(env)).getFrame();
        Vec vec1 = fr.vec(0);

        int sz = fr._names.length;
        String[] colnames = new String[sz];
        int i = 0;
        for (String name : fr._names) colnames[i++] = name;

        //Get the most common value to the second most common value
        ValFrame res = table_counts(vec1, colnames);
        double freqRatio = res.getFrame().vec("Counts").at(0)/res.getFrame().vec("Counts").at(1);

        //Get unique counts
        UniqTask t = new UniqTask().doAll(fr);
        int nUniq = t._uniq.size();
        final AstGroup.G[] uniq = t._uniq.keySet().toArray(new AstGroup.G[nUniq]);
        vec1 = Vec.makeZero(nUniq);
        MRTask unique = new MRTask() {
            @Override
            public void map(Chunk c) {
                int start = (int) c.start();
                for (int i = 0; i < c._len; ++i) c.set(i, uniq[i + start]._gs[0]);
            }
        }.doAll(vec1);
        double uniqueRatio = unique._fr.numRows()/fr.numRows();

        return(res);
    }


    // Count unique values of integers in a column and return top two counts
    private ValFrame table_counts(Vec v1, String[] colnames) {

        //This should be nearly the same cost as a 1-D array, since everything is
        //sparsely filled in.

        //If this is the 1-column case (all counts on the diagonals), just build a
        //1-d result.


        //Slow-pass group counting, very sparse hashtables.
        AstNearZeroVariance.SlowCnt sc = new AstNearZeroVariance.SlowCnt().doAll(v1, v1);

        //Get the column headers as sorted doubles
        double dcols[] = collectDomain(sc._col0s);

        Frame res = new Frame();
        Vec rowlabel = Vec.makeVec(dcols, Vec.VectorGroup.VG_LEN1.addVec());
        rowlabel.setDomain(v1.domain());
        res.add(colnames[0], rowlabel);
        long cnts[] = new long[dcols.length];
        for (int col = 0; col < dcols.length; col++) {
            long lkey = Double.doubleToRawLongBits(dcols[col]);
            NonBlockingHashMapLong<AtomicLong> colx = sc._col0s.get(lkey);
            AtomicLong al = colx.get(lkey);
            cnts[col] = al.get();
        }
        Vec vec = Vec.makeVec(cnts, null, Vec.VectorGroup.VG_LEN1.addVec());
        res.add("Counts", vec);
        int[] sortCol = new int[] {res.find("Counts")}; //Column to sort by. In this case it is "Counts".
        res = res.sort(sortCol); //Return a sorted Frame. Not in place so must reassign.
        //Only interested in highest counts and second highest counts
        long[] rows = new long[] {res.numRows()-1, res.numRows()-2};
        res = res.deepSlice(rows,null);
        return new ValFrame(res);
    }

    //Collect the unique longs from this NBHML, convert to doubles and return
    //them as a sorted double[].
    private static double[] collectDomain(NonBlockingHashMapLong ls) {
        int sz = ls.size();         // Uniques
        double ds[] = new double[sz];
        int x = 0;
        for (NonBlockingHashMapLong.IteratorLong i = iter(ls); i.hasNext(); )
            ds[x++] = Double.longBitsToDouble(i.nextLong());
        Arrays.sort(ds);
        return ds;
    }


    private static NonBlockingHashMapLong.IteratorLong iter(NonBlockingHashMapLong nbhml) {
        return (NonBlockingHashMapLong.IteratorLong) nbhml.keySet().iterator();
    }

    //Implementation is a double-dimension NBHML.  Each dimension key is the raw
    //long bits of the double column.  Bottoms out in an AtomicLong.
    private static class SlowCnt extends MRTask<AstNearZeroVariance.SlowCnt> {
        transient NonBlockingHashMapLong<NonBlockingHashMapLong<AtomicLong>> _col0s;

        @Override
        public void setupLocal() {
            _col0s = new NonBlockingHashMapLong<>();
        }

        @Override
        public void map(Chunk c0, Chunk c1) {
            for (int i = 0; i < c0._len; i++) {

                double d0 = c0.atd(i);
                if (Double.isNaN(d0)) continue;
                long l0 = Double.doubleToRawLongBits(d0);

                double d1 = c1.atd(i);
                if (Double.isNaN(d1)) continue;
                long l1 = Double.doubleToRawLongBits(d1);

                //Atomically fetch/create nested NBHM
                NonBlockingHashMapLong<AtomicLong> col1s = _col0s.get(l0);
                if (col1s == null) {   // Speed filter pre-filled entries
                    col1s = new NonBlockingHashMapLong<>();
                    NonBlockingHashMapLong<AtomicLong> old = _col0s.putIfAbsent(l0, col1s);
                    if (old != null) col1s = old; // Lost race, use old value
                }

                //Atomically fetch/create nested AtomicLong
                AtomicLong cnt = col1s.get(l1);
                if (cnt == null) {   // Speed filter pre-filled entries
                    cnt = new AtomicLong();
                    AtomicLong old = col1s.putIfAbsent(l1, cnt);
                    if (old != null) cnt = old; // Lost race, use old value
                }

                //Atomically bump counter
                cnt.incrementAndGet();
            }
        }

        @Override
        public void reduce(AstNearZeroVariance.SlowCnt sc) {
            if (_col0s == sc._col0s) return;
            throw water.H2O.unimpl();
        }

        public final AutoBuffer write_impl(AutoBuffer ab) {
            if (_col0s == null) return ab.put8(0);
            ab.put8(_col0s.size());
            for (long col0 : _col0s.keySetLong()) {
                ab.put8(col0);
                NonBlockingHashMapLong<AtomicLong> col1s = _col0s.get(col0);
                ab.put8(col1s.size());
                for (long col1 : col1s.keySetLong()) {
                    ab.put8(col1);
                    ab.put8(col1s.get(col1).get());
                }
            }
            return ab;
        }

        public final AstNearZeroVariance.SlowCnt read_impl(AutoBuffer ab) {
            long len0 = ab.get8();
            if (len0 == 0) return this;
            _col0s = new NonBlockingHashMapLong<>();
            for (long i = 0; i < len0; i++) {
                NonBlockingHashMapLong<AtomicLong> col1s = new NonBlockingHashMapLong<>();
                _col0s.put(ab.get8(), col1s);
                long len1 = ab.get8();
                for (long j = 0; j < len1; j++)
                    col1s.put(ab.get8(), new AtomicLong(ab.get8()));
            }
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (NonBlockingHashMapLong.IteratorLong i = iter(_col0s); i.hasNext(); ) {
                long l = i.nextLong();
                double d = Double.longBitsToDouble(l);
                sb.append(d).append(": {");
                NonBlockingHashMapLong<AtomicLong> col1s = _col0s.get(l);
                for (NonBlockingHashMapLong.IteratorLong j = iter(col1s); j.hasNext(); ) {
                    long l2 = j.nextLong();
                    double d2 = Double.longBitsToDouble(l2);
                    AtomicLong al = col1s.get(l2);
                    sb.append(d2).append(": ").append(al.get()).append(", ");
                }
                sb.append("}\n");
            }
            return sb.toString();
        }
    }
    private static class UniqTask extends MRTask<UniqTask> {
        IcedHashMap<AstGroup.G, String> _uniq;

        @Override
        public void map(Chunk[] c) {
            _uniq = new IcedHashMap<>();
            AstGroup.G g = new AstGroup.G(1, null);
            for (int i = 0; i < c[0]._len; ++i) {
                g.fill(i, c, new int[]{0});
                String s_old = _uniq.putIfAbsent(g, "");
                if (s_old == null) g = new AstGroup.G(1, null);
            }
        }

        @Override
        public void reduce(UniqTask t) {
            if (_uniq != t._uniq) {
                IcedHashMap<AstGroup.G, String> l = _uniq;
                IcedHashMap<AstGroup.G, String> r = t._uniq;
                if (l.size() < r.size()) {
                    l = r;
                    r = _uniq;
                }  // larger on the left
                for (AstGroup.G rg : r.keySet()) l.putIfAbsent(rg, "");  // loop over smaller set
                _uniq = l;
                t._uniq = null;
            }
        }
    }
}
