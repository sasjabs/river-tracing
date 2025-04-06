import arcpy


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [Voronoi_Skeleton, Voronoi_Skeleton_Batch, Trace_rivers, Trace_rivers_by_basin, Prepare_rivers]


class Voronoi_Skeleton(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Voronoi Skeleton"
        self.description = ""
        self.canRunInBackground = True

    def getParameterInfo(self):
        in_poly = arcpy.Parameter(
            displayName="Input Polygons",
            name="in_poly",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input")

        comp_dist = arcpy.Parameter(
            displayName="Vertex densifying distance",
            name="comp_dist",
            datatype="Double",
            parameterType="Required",
            direction="Input")
        comp_dist.value = 300

        out_lines = arcpy.Parameter(
            displayName="Output Skeleton",
            name="out_lines",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output")
        
        snap_env = arcpy.Parameter(
            displayName="Snap To",
            name="snap_env",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input")
        
        snap_dist = arcpy.Parameter(
            displayName="Snap distance",
            name="snap_dist",
            datatype="Double",
            parameterType="Required",
            direction="Input")
        snap_dist.value = 1000
        
        params = [in_poly, comp_dist, out_lines, snap_env, snap_dist]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):

        in_poly = parameters[0].valueAsText
        comp_dist = float(parameters[1].valueAsText)
        out_lines = parameters[2].valueAsText
        snap_env = parameters[3].valueAsText
        snap_dist = parameters[4].valueAsText

        pol_dens='in_memory/pol_dens'
        arcpy.GeodeticDensify_management(in_poly,
                                         pol_dens,
                                         "GEODESIC",
                                         str(comp_dist) + " Meters")
        
        pol_lyr = 'pol_lyr'
        arcpy.MakeFeatureLayer_management(pol_dens, pol_lyr)
        
        arcpy.CreateFeatureclass_management('in_memory',
                                            'output',
                                            'POLYLINE',
                                            spatial_reference=arcpy.Describe(in_poly).spatialReference)
        
        output = 'in_memory/output'

        # rows = arcpy.SearchCursor(pol_lyr)
        oids = arcpy.da.TableToNumPyArray(pol_lyr, 'OBJECTID')
        nrows = len(list(oids))
        arcpy.AddMessage(str(nrows) + ' polygons')
        for i, oid in enumerate(oids):
            # polygon_id = row[0]
            polygon_id = oid[0]
            arcpy.AddMessage('Polygon ' + str(i+1) + ' out of ' + str(nrows))
            
            polygon = 'in_memory/polygon'
            arcpy.Select_analysis(pol_dens, polygon, 'OBJECTID = ' + str(polygon_id))
            
            feature_pts='in_memory/feature_pts'
            arcpy.FeatureVerticesToPoints_management(polygon, feature_pts, "ALL")

            voronoi='in_memory/voronoi'
            arcpy.CreateThiessenPolygons_analysis(feature_pts, voronoi, "ONLY_FID")

            sel_lines = 'in_memory/sel_lines'
            arcpy.PolygonToLine_management(voronoi, sel_lines, "IDENTIFY_NEIGHBORS")

            voronoi_lyr = 'voronoi_lyr'
            arcpy.MakeFeatureLayer_management(sel_lines, voronoi_lyr)

            arcpy.SelectLayerByLocation_management(voronoi_lyr, 'COMPLETELY_WITHIN', polygon, selection_type='NEW_SELECTION')
            
            unsplit = 'in_memory/unsplit'
            arcpy.UnsplitLine_management(voronoi_lyr, unsplit)
            
            arcpy.Snap_edit(unsplit, [[snap_env, 'END', snap_dist + ' Meters']])

            arcpy.Append_management(unsplit, output, "NO_TEST")

        arcpy.Select_analysis(output, out_lines) 
        return
    
class Voronoi_Skeleton_Batch(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Voronoi Skeleton Batch"
        self.description = ""
        self.canRunInBackground = True

    def getParameterInfo(self):
        in_poly = arcpy.Parameter(
            displayName="Input Polygons",
            name="in_poly",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input")

        comp_dist = arcpy.Parameter(
            displayName="Vertex densifying distance",
            name="comp_dist",
            datatype="Double",
            parameterType="Required",
            direction="Input")
        comp_dist.value = 300

        out_lines = arcpy.Parameter(
            displayName="Output Skeleton",
            name="out_lines",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output")
        
        snap_env = arcpy.Parameter(
            displayName="Snap To",
            name="snap_env",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input")
        
        snap_dist = arcpy.Parameter(
            displayName="Snap distance",
            name="snap_dist",
            datatype="Double",
            parameterType="Required",
            direction="Input")
        snap_dist.value = 1000
        
        batch_size = arcpy.Parameter(
            displayName="Batch size",
            name="batch_size",
            datatype="GPLong",
            parameterType="Required",
            direction="Input")
        batch_size.value = 500
        
        params = [in_poly, comp_dist, out_lines, snap_env, snap_dist, batch_size]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):

        in_poly = parameters[0].valueAsText
        comp_dist = float(parameters[1].valueAsText)
        out_lines = parameters[2].valueAsText
        snap_env = parameters[3].valueAsText
        snap_dist = parameters[4].valueAsText
        batch_size = int(parameters[5].valueAsText)

        arcpy.AddMessage('Densifying polygons...')
        pol_dens='in_memory/pol_dens'
        arcpy.GeodeticDensify_management(in_poly,
                                         pol_dens,
                                         "GEODESIC",
                                         str(comp_dist) + " Meters")
        
        arcpy.AddMessage('Sorting...')
        pol_sort = 'in_memory/pol_sort'
        arcpy.Sort_management(pol_dens, pol_sort, [['Shape', 'Ascending']], 'PEANO')
        
        arcpy.CreateFeatureclass_management('in_memory',
                                            'output',
                                            'POLYLINE',
                                            spatial_reference=arcpy.Describe(in_poly).spatialReference)
        
        output = 'in_memory/output'

        # rows = arcpy.SearchCursor(pol_lyr)
        oids_raw = arcpy.da.TableToNumPyArray(pol_sort, 'OBJECTID')
        oids = []
        for oid in oids_raw:
            oids.append(oid[0])
        
        n_batches = len(oids) // batch_size
        if len(oids) % batch_size != 0:
            n_batches += 1
        arcpy.AddMessage("Batch size: " + str(batch_size))
        arcpy.AddMessage("Number of batches: " + str(n_batches))
            
        for i in range(n_batches):
            arcpy.AddMessage('Processing batch ' + str(i+1) + ' out of ' + str(n_batches))
            
            batch_oids = oids[i*batch_size:(i+1)*batch_size]
            
            batch = 'in_memory/batch'
            oids_str = str(tuple(batch_oids))
            if len(batch_oids) < 2:
                oids_str = oids_str.replace(',', '')
            arcpy.Select_analysis(pol_sort, batch, 'OBJECTID IN ' + oids_str)
            
            feature_pts='in_memory/feature_pts'
            arcpy.FeatureVerticesToPoints_management(batch, feature_pts, "ALL")

            voronoi='in_memory/voronoi'
            arcpy.CreateThiessenPolygons_analysis(feature_pts, voronoi, "ONLY_FID")

            sel_lines = 'in_memory/sel_lines'
            arcpy.PolygonToLine_management(voronoi, sel_lines, "IDENTIFY_NEIGHBORS")

            voronoi_lyr = 'voronoi_lyr'
            arcpy.MakeFeatureLayer_management(sel_lines, voronoi_lyr)

            arcpy.SelectLayerByLocation_management(voronoi_lyr, 'COMPLETELY_WITHIN', batch, selection_type='NEW_SELECTION')
            
            unsplit = 'in_memory/unsplit'
            arcpy.UnsplitLine_management(voronoi_lyr, unsplit)
            
            arcpy.Snap_edit(unsplit, [[snap_env, 'END', snap_dist + ' Meters']])

            arcpy.Append_management(unsplit, output, "NO_TEST")

        arcpy.Select_analysis(output, out_lines) 
        return

class Trace_rivers(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Trace Rivers"
        self.description = ""
        self.canRunInBackground = True

    def getParameterInfo(self):
        nd = arcpy.Parameter(
            displayName="Network Dataset",
            name="nd",
            datatype="DENetworkDataset",
            parameterType="Required",
            direction="Input"
        )
        
        source_pts = arcpy.Parameter(
            displayName="Source points",
            name="source_pts",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        
        mouth_pts = arcpy.Parameter(
            displayName="Mouth points",
            name="mouth_pts",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        
        out_lines = arcpy.Parameter(
            displayName="Output Rivers",
            name="out_lines",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output"
        )
        
        batch_size = arcpy.Parameter(
            displayName="Batch size",
            name="batch_size",
            datatype="GPLong",
            parameterType="Required",
            direction="Input")
        batch_size.value = 500

        params = [nd, source_pts, mouth_pts, out_lines, batch_size]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        nd = parameters[0].valueAsText
        # nd_junctions = parameters[1].valueAsText
        source_pts = parameters[1].valueAsText
        mouth_pts = parameters[2].valueAsText
        out_lines = parameters[3].valueAsText
        batch_size = int(parameters[4].valueAsText)
        
        arcpy.AddMessage("Creating Closest facility layer...")
        nd_lyr = 'nd_lyr'
        arcpy.MakeClosestFacilityLayer_na(nd, nd_lyr, 'Length', travel_from_to='TRAVEL_FROM', default_number_facilities_to_find=1, hierarchy='USE_HIERARCHY')

        arcpy.AddMessage("Adding locations...")
        arcpy.AddLocations_na(nd_lyr, 'Facilities', mouth_pts, "", "")
        
        arcpy.CreateFeatureclass_management('in_memory',
                                            'routes',
                                            'POLYLINE',
                                            spatial_reference=arcpy.Describe(nd).spatialReference)
        
        routes = 'in_memory/routes'
        
        arcpy.AddMessage("Counting sources...")
        oids_raw = arcpy.da.TableToNumPyArray(source_pts, 'OBJECTID')
        oids = []
        for oid in oids_raw:
            oids.append(oid[0])
        
        n_batches = len(oids) // batch_size
        if len(oids) % batch_size != 0:
            n_batches += 1
        arcpy.AddMessage("Batch size: " + str(batch_size))
        arcpy.AddMessage("Number of batches: " + str(n_batches))
            
        for i in range(n_batches):
            arcpy.AddMessage('Processing batch ' + str(i+1) + ' out of ' + str(n_batches))
            
            batch_oids = oids[i*batch_size:(i+1)*batch_size]
            
            batch = 'in_memory/batch'
            arcpy.Select_analysis(source_pts, batch, 'OBJECTID IN ' + str(tuple(batch_oids)))
            
            arcpy.AddMessage('\t Solving...')
            arcpy.AddLocations_na(nd_lyr, 'Incidents', batch, "", "", append="CLEAR")
            arcpy.Solve_na(nd_lyr)
            
            arcpy.AddMessage('\t Splitting lines...')
            
            ftl = 'in_memory/ftl'
            arcpy.FeatureToLine_management('Routes', ftl)
            
            arcpy.AddMessage('\t Deleting duplicates...')
            arcpy.DeleteIdentical_management(ftl, 'Shape', "1 Meter")
            
            arcpy.Append_management(ftl, routes, "NO_TEST")
        
        arcpy.AddMessage('Planarizing lines...')
        arcpy.FeatureToLine_management(routes, out_lines, "10 Meters")
        
        arcpy.AddMessage('Deleting Identical features...')
        arcpy.DeleteIdentical_management(out_lines, 'Shape', "10 Meters")
        
        arcpy.AddMessage('Flipping lines...')
        arcpy.FlipLine_edit(out_lines)
        return
    
class Trace_rivers_by_basin(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Trace Rivers by Basin"
        self.description = ""
        self.canRunInBackground = True

    def getParameterInfo(self):
        nd = arcpy.Parameter(
            displayName="Network Dataset",
            name="nd",
            datatype="DENetworkDataset",
            parameterType="Required",
            direction="Input"
        )
        
        source_pts = arcpy.Parameter(
            displayName="Source points",
            name="source_pts",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        
        mouth_pts = arcpy.Parameter(
            displayName="Mouth points",
            name="mouth_pts",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        
        out_lines = arcpy.Parameter(
            displayName="Output Rivers",
            name="out_lines",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output"
        )
        
        batch_size = arcpy.Parameter(
            displayName="Batch size",
            name="batch_size",
            datatype="GPLong",
            parameterType="Required",
            direction="Input")
        batch_size.value = 500
        
        basins = arcpy.Parameter(
            displayName="Basin polygons",
            name="basins",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )

        params = [nd, source_pts, mouth_pts, out_lines, batch_size, basins]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        nd = parameters[0].valueAsText
        source_pts = parameters[1].valueAsText
        mouth_pts = parameters[2].valueAsText
        out_lines = parameters[3].valueAsText
        batch_size = int(parameters[4].valueAsText)
        basins = parameters[5].valueAsText
        
        arcpy.AddMessage("Creating Closest facility layer...")
        nd_lyr = 'nd_lyr'
        arcpy.MakeClosestFacilityLayer_na(nd, nd_lyr, 'Length', travel_from_to='TRAVEL_FROM', default_number_facilities_to_find=1, hierarchy='USE_HIERARCHY')
        
        arcpy.CreateFeatureclass_management('in_memory',
                                            'routes',
                                            'POLYLINE',
                                            spatial_reference=arcpy.Describe(nd).spatialReference)
        
        routes = 'in_memory/routes'
        
        src_lyr = 'src_lyr'
        arcpy.MakeFeatureLayer_management(source_pts, src_lyr)
        mth_lyr = 'mth_lyr'
        arcpy.MakeFeatureLayer_management(mouth_pts, mth_lyr)
        
        cursor =  arcpy.da.SearchCursor(basins, ['SHAPE@', 'OBJECTID'])
        nbasins = len(list(cursor))
        arcpy.AddMessage(str(nbasins) + ' basins')
        cursor =  arcpy.da.SearchCursor(basins, ['SHAPE@', 'OBJECTID'])
        
        for row in cursor:
            print("Jopa")
            bas_id = row[1]
            arcpy.AddMessage("Processing basin " + str(bas_id) + " out of " + str(nbasins))
            
            basin = 'in_memory/basin'
            arcpy.Select_analysis(basins, basin, 'OBJECTID = ' + str(bas_id))
            
            arcpy.SelectLayerByLocation_management(src_lyr, 'INTERSECT', basin, "", "NEW_SELECTION")
            arcpy.SelectLayerByLocation_management(mth_lyr, 'INTERSECT', basin, "5 kilometers", "NEW_SELECTION")
            
            src_sel = 'in_memory/src_sel'
            mth_sel = 'in_memory/mth_sel'
            arcpy.Select_analysis(src_lyr, src_sel)
            arcpy.Select_analysis(mth_lyr, mth_sel)
            
            arcpy.AddLocations_na(nd_lyr, 'Facilities', mth_sel, "", "")
            
            arcpy.AddMessage("Counting sources...")
            oids_raw = arcpy.da.TableToNumPyArray(src_sel, 'OBJECTID')
            oids = []
            for oid in oids_raw:
                oids.append(oid[0])
            
            n_batches = len(oids) // batch_size
            if len(oids) % batch_size != 0:
                n_batches += 1
            arcpy.AddMessage("Batch size: " + str(batch_size))
            arcpy.AddMessage("Number of batches: " + str(n_batches))
                
            for i in range(n_batches):
                arcpy.AddMessage('Processing batch ' + str(i+1) + ' out of ' + str(n_batches))
                
                batch_oids = oids[i*batch_size:(i+1)*batch_size]
                
                batch = 'in_memory/batch'
                arcpy.Select_analysis(src_sel, batch, 'OBJECTID IN ' + str(tuple(batch_oids)))
                
                arcpy.AddMessage('\t Solving...')
                arcpy.AddLocations_na(nd_lyr, 'Incidents', batch, "", "", append="CLEAR")
                arcpy.Solve_na(nd_lyr)
                
                arcpy.AddMessage('\t Splitting lines...')
                
                ftl = 'in_memory/ftl'
                arcpy.FeatureToLine_management('Routes', ftl)
                
                arcpy.AddMessage('\t Deleting duplicates...')
                arcpy.DeleteIdentical_management(ftl, 'Shape', "1 Meter")
                
                arcpy.Append_management(ftl, routes, "NO_TEST")
        
        arcpy.AddMessage('Planarizing lines...')
        arcpy.FeatureToLine_management(routes, out_lines, "10 Meters")
        
        arcpy.AddMessage('Deleting Identical features...')
        arcpy.DeleteIdentical_management(out_lines, 'Shape', "10 Meters")
        
        arcpy.AddMessage('Flipping lines...')
        arcpy.FlipLine_edit(out_lines)
        return
    
    
class Prepare_rivers(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Prepare rivers"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        
        in_rivers = arcpy.Parameter(
            displayName="Input rivers",
            name="in_rivers",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        
        water_bodies = arcpy.Parameter(
            displayName="Water bodies",
            name="water_bodies",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        
        mouth_pts = arcpy.Parameter(
            displayName="Mouth points",
            name="mouth_pts",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        
        out_lines = arcpy.Parameter(
            displayName="Output Rivers",
            name="out_lines",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output"
        )
        
        niter = arcpy.Parameter(
            displayName="Number of iterations",
            name="niter",
            datatype="GPLong",
            parameterType="Required",
            direction="Input")
        niter.value = 3

        params = [in_rivers, water_bodies, mouth_pts, out_lines, niter]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        in_rivers = parameters[0].valueAsText
        water_bodies = parameters[1].valueAsText
        mouth_pts = parameters[2].valueAsText
        out_lines= parameters[3].valueAsText
        niter = int(parameters[4].valueAsText)
        
        arcpy.AddMessage('Planarizing lines...')
        rivers_processing = 'in_memory/rivers_processing'
        arcpy.FeatureToLine_management(in_rivers, rivers_processing, "10 meters")
        
        arcpy.AddMessage('Deleting segments inside waterbodies...')
        for i in range(niter):
            
            arcpy.AddMessage('\t Iteration ' + str(i+1) + ' out of ' + str(niter))
            arcpy.AddMessage('\t Retrieving dangles...')
            dangles = 'in_memory/dangles'
            arcpy.FeatureVerticesToPoints_management(rivers_processing, dangles, 'DANGLE')
            
            arcpy.AddMessage('\t Selecting dangles inside waterbodies...')
            dangles_lyr = 'dangles_lyr'
            arcpy.MakeFeatureLayer_management(dangles, dangles_lyr)
            arcpy.SelectLayerByLocation_management(dangles_lyr, 'WITHIN_CLEMENTINI', water_bodies, selection_type='NEW_SELECTION')
            arcpy.SelectLayerByLocation_management(dangles_lyr, 'ARE_IDENTICAL_TO', mouth_pts, selection_type='REMOVE_FROM_SELECTION')
            
            arcpy.AddMessage('\t Selecting river segments to delete...')
            rivers_lyr = 'rivers_lyr'
            arcpy.MakeFeatureLayer_management(rivers_processing, rivers_lyr)
            arcpy.SelectLayerByLocation_management(rivers_lyr, 'INTERSECT', dangles_lyr, selection_type='NEW_SELECTION')
            arcpy.SelectLayerByLocation_management(rivers_lyr, selection_type='SWITCH_SELECTION')
            
            arcpy.AddMessage('\t Unsplitting...')
            unsplit = 'in_memory/unsplit'
            arcpy.UnsplitLine_management(rivers_lyr, unsplit)
            arcpy.Select_analysis(unsplit, rivers_processing)
        
        arcpy.AddMessage('Saving result...')
        arcpy.FeatureToLine_management(rivers_processing, out_lines)
        return