/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ControlFlag;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

@Component(metatype = true,
        label = "CUG Configuration",
        description = "Component to enable Allows to exclude principal(s) with the configured name(s) from CUG evaluation.",
        policy = ConfigurationPolicy.REQUIRE)
@Service({AuthorizationConfiguration.class, SecurityConfiguration.class})
@Properties({
        @Property(name = CugConstants.PARAM_CUG_SUPPORTED_PATHS,
                label = "Supported Paths",
                description = "Paths under which CUGs can be created and will be evaluated.",
                cardinality = Integer.MAX_VALUE),
        @Property(name = CugConstants.PARAM_CUG_ENABLED,
                label = "CUG Enabled",
                description = "Flag to enable the evaluation of the configured CUG policies.",
                boolValue = false),
        @Property(name = CompositeConfiguration.PARAM_RANKING,
                label = "Ranking",
                description = "Ranking of this configuration in a setup with multiple authorization configurations.",
                intValue = 200),
        @Property(name = AggregatedPermissionProvider.PARAM_CONTROL_FLAG,
                label = "Control Flag",
                description = "Control flag defining if the permission provider is SUFFICIENT or REQUISITE.",
                options = {
                        @PropertyOption(name = ControlFlag.SUFFICIENT_NAME, value = ControlFlag.SUFFICIENT_NAME),
                        @PropertyOption(name = ControlFlag.REQUISITE_NAME, value = ControlFlag.REQUISITE_NAME)
                },
                value = ControlFlag.REQUISITE_NAME)
})
public class CugConfiguration extends ConfigurationBase implements AuthorizationConfiguration, CugConstants {

    @Reference
    private ContentRepository repository;

    /**
     * Reference to services implementing {@link org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude}.
     */
    @Reference
    private CugExclude exclude = new CugExclude.Default();

    public CugConfiguration() {
        super();
    }

    public CugConfiguration(@Nonnull SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    @Override
    public AccessControlManager getAccessControlManager(Root root, NamePathMapper namePathMapper) {
        return new CugAccessControlManager(root, namePathMapper, getSecurityProvider());
    }

    @Override
    public RestrictionProvider getRestrictionProvider() {
        return RestrictionProvider.EMPTY;
    }

    @Override
    public PermissionProvider getPermissionProvider(Root root, String workspaceName, Set<Principal> principals) {
        ConfigurationParameters params = getParameters();
        boolean enabled = params.getConfigValue(CugConstants.PARAM_CUG_ENABLED, false);

        String[] supportedPaths = params.getConfigValue(CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[0]);
        if (!enabled || supportedPaths.length == 0 || getExclude().isExcluded(principals)) {
            return EmptyPermissionProvider.getInstance();
        } else {
            ControlFlag flag = ControlFlag.valueOf(params.getConfigValue(AggregatedPermissionProvider.PARAM_CONTROL_FLAG, ControlFlag.REQUISITE_NAME));
            return new CugPermissionProvider(root, principals, supportedPaths, flag, getContext());
        }
    }

    @Override
    public String getName() {
        return AuthorizationConfiguration.NAME;
    }

    @Nonnull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new RepositoryInitializer() {
            @Override
            public void initialize(NodeBuilder builder) {
                NodeState base = builder.getNodeState();
                NodeStore store = new MemoryNodeStore(base);

                Root root = RootFactory.createSystemRoot(store,
                        new EditorHook(new CompositeEditorProvider(new NamespaceEditorProvider(), new TypeEditorProvider())),
                        null, null, null, null);
                if (CugUtil.registerCugNodeTypes(root)) {
                    NodeState target = store.getRoot();
                    target.compareAgainstBaseState(base, new ApplyDiff(builder));
                }
            }
        };
    }

    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.<ProtectedItemImporter>singletonList(new CugImporter());
    }

    @Override
    public Context getContext() {
        return CugContext.INSTANCE;
    }

    //----------------------------------------------------< SCR Integration >---

    @Activate
    protected void activate() throws IOException, CommitFailedException, PrivilegedActionException, RepositoryException {
        ContentSession systemSession = null;
        try {
            systemSession = Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<ContentSession>() {
                @Override
                public ContentSession run() throws LoginException, RepositoryException {
                    return repository.login(null, null);
                }
            });
            final Root root = systemSession.getLatestRoot();
            if (CugUtil.registerCugNodeTypes(root)) {
                root.commit();
            }
        } finally {
            if (systemSession != null) {
                systemSession.close();
            }
        }
    }

    //--------------------------------------------------------------------------
    private CugExclude getExclude() {
        return (exclude == null) ? new CugExclude.Default() : exclude;
    }
}