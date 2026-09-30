package fixture;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;

import javax.persistence.EntityManager;
import javax.persistence.NamedStoredProcedureQuery;

import org.springframework.data.jpa.repository.Procedure;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;

/**
 * One method per call shape the DB-call detector must recognise. Compiled at test time by
 * ScratchCompiler; the test parses the resulting .class files and asserts on the detector output.
 */
@NamedStoredProcedureQuery(name = "OrderSummary", procedureName = "PKG.ORDER_SUMMARY")
public class DbCallShapes {

    /** JDBC: {call PKG.PROC(?,?)} — two bind parameters. */
    public void callable(Connection c) throws Exception {
        CallableStatement cs = c.prepareCall("{call PKG.PROC(?,?)}");
        cs.execute();
    }

    /** JDBC: {? = call PKG.FUNC(?)} — a function call with one bind parameter. */
    public void functionCall(Connection c) throws Exception {
        CallableStatement cs = c.prepareCall("{? = call PKG.FUNC(?)}");
        cs.executeQuery();
    }

    /** JDBC: a PL/SQL anonymous block in prepareStatement. */
    public void plsqlBlock(Connection c) throws Exception {
        PreparedStatement ps = c.prepareStatement("begin PKG.PROC; end;");
        ps.execute();
    }

    /** Plain SQL: a schema-qualified function in the SELECT column list. */
    public void selectFunction(Connection c) throws Exception {
        PreparedStatement ps = c.prepareStatement("SELECT PKG.FUNC(x) FROM DUAL");
        ps.executeQuery();
    }

    /** Spring JDBC: SimpleJdbcCall fluent wiring (catalog + procedure name). */
    public void springJdbc() {
        SimpleJdbcCall call = new SimpleJdbcCall(null)
                .withCatalogName("PKG")
                .withProcedureName("PROC");
        call.execute();
    }

    /** JPA: EntityManager.createStoredProcedureQuery. */
    public void jpaProc(EntityManager em) {
        em.createStoredProcedureQuery("PKG.JPA_PROC");
    }

    /** JPA: @Procedure annotation. */
    @Procedure(name = "PKG.ANNOTATED_PROC")
    public void annotatedProc() {
    }
}
